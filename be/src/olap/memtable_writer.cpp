// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/memtable_writer.h"

#include <fmt/format.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

MemTableWriter::MemTableWriter(const WriteRequest& req, RuntimeProfile* profile) : _req(req) {
    _init_profile(profile);
}

void MemTableWriter::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("MemTableWriter {}", _req.tablet_id), true, true);
    _lock_timer = ADD_TIMER(_profile, "LockTime");
    _sort_timer = ADD_TIMER(_profile, "MemTableSortTime");
    _agg_timer = ADD_TIMER(_profile, "MemTableAggTime");
    _memtable_duration_timer = ADD_TIMER(_profile, "MemTableDurationTime");
    _segment_writer_timer = ADD_TIMER(_profile, "SegmentWriterTime");
    _wait_flush_timer = ADD_TIMER(_profile, "MemTableWaitFlushTime");
    _put_into_output_timer = ADD_TIMER(_profile, "MemTablePutIntoOutputTime");
    _delete_bitmap_timer = ADD_TIMER(_profile, "DeleteBitmapTime");
    _close_wait_timer = ADD_TIMER(_profile, "MemTableWriterCloseWaitTime");
    _sort_times = ADD_COUNTER(_profile, "MemTableSortTimes", TUnit::UNIT);
    _agg_times = ADD_COUNTER(_profile, "MemTableAggTimes", TUnit::UNIT);
    _segment_num = ADD_COUNTER(_profile, "SegmentNum", TUnit::UNIT);
    _raw_rows_num = ADD_COUNTER(_profile, "RawRowNum", TUnit::UNIT);
    _merged_rows_num = ADD_COUNTER(_profile, "MergedRowNum", TUnit::UNIT);
}

MemTableWriter::~MemTableWriter() {
    if (!_is_init) {
        return;
    }
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _mem_table.reset();
}

Status MemTableWriter::init(std::shared_ptr<RowsetWriter> rowset_writer,
                            TabletSchemaSPtr tablet_schema, bool unique_key_mow) {
    _rowset_writer = rowset_writer;
    _tablet_schema = tablet_schema;
    _unique_key_mow = unique_key_mow;

    _reset_mem_table();

    // create flush handler
    // by assigning segment_id to memtable before submiting to flush executor,
    // we can make sure same keys sort in the same order in all replicas.
    bool should_serial = false;
    RETURN_IF_ERROR(StorageEngine::instance()->memtable_flush_executor()->create_flush_token(
            _flush_token, _rowset_writer.get(), should_serial, _req.is_high_priority));

    _is_init = true;
    return Status::OK();
}

Status MemTableWriter::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status MemTableWriter::write(const vectorized::Block* block, const std::vector<int>& row_idxs,
                             bool is_append) {
    if (UNLIKELY(row_idxs.empty() && !is_append)) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (_is_cancelled) {
        return _cancel_status;
    }
    if (!_is_init) {
        return Status::Error<NOT_INITIALIZED>("delta segment writer has not been initialized");
    }
    if (_is_closed) {
        return Status::Error<ALREADY_CLOSED>("write block after closed tablet_id={}, load_id={}-{}",
                                             _req.tablet_id, _req.load_id.hi(), _req.load_id.lo());
    }

    if (is_append) {
        _total_received_rows += block->rows();
    } else {
        _total_received_rows += row_idxs.size();
    }
    _mem_table->insert(block, row_idxs, is_append);

    if (UNLIKELY(_mem_table->need_agg() && config::enable_shrink_memory)) {
        _mem_table->shrink_memtable_by_agg();
    }
    if (UNLIKELY(_mem_table->need_flush())) {
        auto s = _flush_memtable_async();
        _reset_mem_table();
        if (UNLIKELY(!s.ok())) {
            return s;
        }
    }

    return Status::OK();
}

Status MemTableWriter::_flush_memtable_async() {
    DCHECK(_flush_token != nullptr);
    return _flush_token->submit(std::move(_mem_table));
}

Status MemTableWriter::flush_memtable_and_wait(bool need_wait) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_is_init) {
        // This writer is not initialized before flushing. Do nothing
        // But we return OK instead of Status::Error<ALREADY_CANCELLED>(),
        // Because this method maybe called when trying to reduce mem consumption,
        // and at that time, the writer may not be initialized yet and that is a normal case.
        return Status::OK();
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    VLOG_NOTICE << "flush memtable to reduce mem consumption. memtable size: "
                << _mem_table->memory_usage() << ", tablet: " << _req.tablet_id
                << ", load id: " << print_id(_req.load_id);
    auto s = _flush_memtable_async();
    _reset_mem_table();
    if (UNLIKELY(!s.ok())) {
        return s;
    }

    if (need_wait) {
        // wait all memtables in flush queue to be flushed.
        SCOPED_TIMER(_wait_flush_timer);
        RETURN_IF_ERROR(_flush_token->wait());
    }
    return Status::OK();
}

Status MemTableWriter::wait_flush() {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (!_is_init) {
            // return OK instead of Status::Error<ALREADY_CANCELLED>() for same reason
            // as described in flush_memtable_and_wait()
            return Status::OK();
        }
        if (_is_cancelled) {
            return _cancel_status;
        }
    }
    SCOPED_TIMER(_wait_flush_timer);
    RETURN_IF_ERROR(_flush_token->wait());
    return Status::OK();
}

void MemTableWriter::_reset_mem_table() {
#ifndef BE_TEST
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num,
                        UniqueId(_req.load_id).to_string()),
            ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker());
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++,
                        UniqueId(_req.load_id).to_string()),
            ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker());
#else
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(fmt::format(
            "MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
            std::to_string(tablet_id()), _mem_table_num, UniqueId(_req.load_id).to_string()));
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(fmt::format(
            "MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}", std::to_string(tablet_id()),
            _mem_table_num++, UniqueId(_req.load_id).to_string()));
#endif
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        _mem_table_insert_trackers.push_back(mem_table_insert_tracker);
        _mem_table_flush_trackers.push_back(mem_table_flush_tracker);
    }
    _mem_table.reset(new MemTable(_req.tablet_id, _tablet_schema.get(), _req.slots, _req.tuple_desc,
                                  _unique_key_mow, mem_table_insert_tracker,
                                  mem_table_flush_tracker));

    COUNTER_UPDATE(_segment_num, 1);
}

Status MemTableWriter::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (_is_cancelled) {
        return _cancel_status;
    }
    if (!_is_init) {
        return Status::Error<NOT_INITIALIZED>("delta segment writer has not been initialized");
    }
    if (_is_closed) {
        LOG(WARNING) << "close after closed tablet_id=" << _req.tablet_id
                     << " load_id=" << _req.load_id;
        return Status::OK();
    }

    auto s = _flush_memtable_async();
    _mem_table.reset();
    _is_closed = true;
    if (UNLIKELY(!s.ok())) {
        return s;
    } else {
        return Status::OK();
    }
}

Status MemTableWriter::close_wait() {
    SCOPED_TIMER(_close_wait_timer);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (_is_cancelled) {
        return _cancel_status;
    }

    Status st;
    // return error if previous flush failed
    {
        SCOPED_TIMER(_wait_flush_timer);
        st = _flush_token->wait();
    }
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "previous flush failed tablet " << _req.tablet_id;
        return st;
    }

    _mem_table.reset();

    if (_rowset_writer->num_rows() + _flush_token->memtable_stat().merged_rows !=
        _total_received_rows) {
        LOG(WARNING) << "the rows number written doesn't match, rowset num rows written to file: "
                     << _rowset_writer->num_rows()
                     << ", merged_rows: " << _flush_token->memtable_stat().merged_rows
                     << ", total received rows: " << _total_received_rows;
        return Status::InternalError("rows number written by delta writer dosen't match");
    }

    // const FlushStatistic& stat = _flush_token->get_stats();
    // print slow log if wait more than 1s
    /*if (_wait_flush_timer->elapsed_time() > 1000UL * 1000 * 1000) {
        LOG(INFO) << "close delta writer for tablet: " << req.tablet_id
                  << ", load id: " << print_id(_req.load_id) << ", wait close for "
                  << _wait_flush_timer->elapsed_time() << "(ns), stats: " << stat;
    }*/

    COUNTER_UPDATE(_lock_timer, _lock_watch.elapsed_time() / 1000);
    COUNTER_SET(_delete_bitmap_timer, _rowset_writer->delete_bitmap_ns());
    COUNTER_SET(_segment_writer_timer, _rowset_writer->segment_writer_ns());
    const auto& memtable_stat = _flush_token->memtable_stat();
    COUNTER_SET(_sort_timer, memtable_stat.sort_ns);
    COUNTER_SET(_agg_timer, memtable_stat.agg_ns);
    COUNTER_SET(_memtable_duration_timer, memtable_stat.duration_ns);
    COUNTER_SET(_put_into_output_timer, memtable_stat.put_into_output_ns);
    COUNTER_SET(_sort_times, memtable_stat.sort_times);
    COUNTER_SET(_agg_times, memtable_stat.agg_times);
    COUNTER_SET(_raw_rows_num, memtable_stat.raw_rows);
    COUNTER_SET(_merged_rows_num, memtable_stat.merged_rows);
    return Status::OK();
}

Status MemTableWriter::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status MemTableWriter::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _is_cancelled = true;
    _cancel_status = st;
    return Status::OK();
}

const FlushStatistic& MemTableWriter::get_flush_token_stats() {
    return _flush_token->get_stats();
}

int64_t MemTableWriter::mem_consumption(MemType mem) {
    if (_flush_token == nullptr) {
        // This method may be called before this writer is initialized.
        // So _flush_token may be null.
        return 0;
    }
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        if ((mem & MemType::WRITE) == MemType::WRITE) { // 3 & 2 = 2
            for (auto mem_table_tracker : _mem_table_insert_trackers) {
                mem_usage += mem_table_tracker->consumption();
            }
        }
        if ((mem & MemType::FLUSH) == MemType::FLUSH) { // 3 & 1 = 1
            for (auto mem_table_tracker : _mem_table_flush_trackers) {
                mem_usage += mem_table_tracker->consumption();
            }
        }
    }
    return mem_usage;
}

int64_t MemTableWriter::active_memtable_mem_consumption() {
    if (_flush_token == nullptr) {
        // This method may be called before this writer is initialized.
        // So _flush_token may be null.
        return 0;
    }
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        if (_mem_table_insert_trackers.size() > 0) {
            mem_usage += (*_mem_table_insert_trackers.rbegin())->consumption();
            mem_usage += (*_mem_table_flush_trackers.rbegin())->consumption();
        }
    }
    return mem_usage;
}

} // namespace doris
