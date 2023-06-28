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

#include "olap/delta_writer_v2.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

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
#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "io/fs/stream_sink_file_writer.h"
#include "olap/data_dir.h"
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

Status DeltaWriterV2::open(WriteRequest* req, DeltaWriterV2** writer, RuntimeProfile* profile,
                         const UniqueId& load_id) {
    *writer = new DeltaWriterV2(req, StorageEngine::instance(), profile, load_id);
    return Status::OK();
}

DeltaWriterV2::DeltaWriterV2(WriteRequest* req, StorageEngine* storage_engine, RuntimeProfile* profile,
                         const UniqueId& load_id)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(new TabletSchema),
          _delta_written_success(false),
          _storage_engine(storage_engine),
          _load_id(load_id) {
    _init_profile(profile);
}

void DeltaWriterV2::_init_profile(RuntimeProfile* profile) {
    _profile = profile->create_child(fmt::format("DeltaWriterV2 {}", _req.tablet_id), true, true);
    _lock_timer = ADD_TIMER(_profile, "LockTime");
    _sort_timer = ADD_TIMER(_profile, "MemTableSortTime");
    _agg_timer = ADD_TIMER(_profile, "MemTableAggTime");
    _memtable_duration_timer = ADD_TIMER(_profile, "MemTableDurationTime");
    _segment_writer_timer = ADD_TIMER(_profile, "SegmentWriterTime");
    _wait_flush_timer = ADD_TIMER(_profile, "MemTableWaitFlushTime");
    _put_into_output_timer = ADD_TIMER(_profile, "MemTablePutIntoOutputTime");
    _delete_bitmap_timer = ADD_TIMER(_profile, "MemTableDeleteBitmapTime");
    _close_wait_timer = ADD_TIMER(_profile, "DeltaWriterV2CloseWaitTime");
    _rowset_build_timer = ADD_TIMER(_profile, "RowsetBuildTime");
    _commit_txn_timer = ADD_TIMER(_profile, "CommitTxnTime");
    _sort_times = ADD_COUNTER(_profile, "MemTableSortTimes", TUnit::UNIT);
    _agg_times = ADD_COUNTER(_profile, "MemTableAggTimes", TUnit::UNIT);
    _segment_num = ADD_COUNTER(_profile, "SegmentNum", TUnit::UNIT);
    _raw_rows_num = ADD_COUNTER(_profile, "RawRowNum", TUnit::UNIT);
    _merged_rows_num = ADD_COUNTER(_profile, "MergedRowNum", TUnit::UNIT);
}

DeltaWriterV2::~DeltaWriterV2() {
    if (_is_init && !_delta_written_success) {
        _garbage_collection();
    }

    if (!_is_init) {
        return;
    }

    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();

        if (_tablet != nullptr) {
            const FlushStatistic& stat = _flush_token->get_stats();
            _tablet->flush_bytes->increment(stat.flush_size_bytes);
            _tablet->flush_finish_count->increment(stat.flush_finish_count);
        }
    }

    if (_tablet != nullptr) {
        _tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                _rowset_writer->rowset_id().to_string());
    }

    _mem_table.reset();
}

void DeltaWriterV2::_garbage_collection() {
    Status rollback_status = Status::OK();
    TxnManager* txn_mgr = _storage_engine->txn_manager();
    if (_tablet != nullptr) {
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, _tablet, _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

Status DeltaWriterV2::init() {
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_req.tablet_id);
    if (_tablet == nullptr) {
        LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id
                     << ", schema_hash=" << _req.schema_hash;
        return Status::Error<TABLE_NOT_FOUND>();
    }

    // get rowset ids snapshot
    if (_tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::shared_mutex> lck(_tablet->get_header_lock());
        _cur_max_version = _tablet->max_version_unlocked().second;
        _rowset_ids = _tablet->all_rs_id(_cur_max_version);
    }

    // check tablet version number
    if (!config::disable_auto_compaction &&
        _tablet->exceed_version_limit(config::max_tablet_version_num - 100) &&
        !MemInfo::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
        //trigger compaction
        StorageEngine::instance()->submit_compaction_task(
                _tablet, CompactionType::CUMULATIVE_COMPACTION, true);
        if (_tablet->version_count() > config::max_tablet_version_num) {
            LOG(WARNING) << "failed to init delta writer. version count: "
                         << _tablet->version_count()
                         << ", exceed limit: " << config::max_tablet_version_num
                         << ". tablet: " << _tablet->full_name();
            return Status::Error<TOO_MANY_VERSION>();
        }
    }

    {
        std::shared_lock base_migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!base_migration_rlock.owns_lock()) {
            return Status::Error<TRY_LOCK_FAILED>();
        }
        std::lock_guard<std::mutex> push_lock(_tablet->get_push_lock());
        RETURN_IF_ERROR(_storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet,
                                                                    _req.txn_id, _req.load_id));
    }
    if (_tablet->enable_unique_key_merge_on_write() && _delete_bitmap == nullptr) {
        _delete_bitmap.reset(new DeleteBitmap(_tablet->tablet_id()));
    }
    // build tablet schema in request level
    _build_current_tablet_schema(_req.index_id, _req.table_schema_param, *_tablet->tablet_schema());
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.index_id = _req.index_id;
    context.partition_id = _req.partition_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _tablet->table_id();
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = std::make_shared<MowContext>(_cur_max_version, _req.txn_id, _rowset_ids,
                                                       _delete_bitmap);
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &_rowset_writer));
    _rowset_writer->add_streams(_streams);

    _schema.reset(new Schema(_tablet_schema));
    _reset_mem_table();

    // create flush handler
    // by assigning segment_id to memtable before submiting to flush executor,
    // we can make sure same keys sort in the same order in all replicas.
    bool should_serial = false;
    RETURN_IF_ERROR(_storage_engine->memtable_flush_executor()->create_flush_token(
            &_flush_token, _rowset_writer->type(), should_serial, _req.is_high_priority));
    _flush_token->register_flying_memtable_counter(_flying_memtable_counter);

    _is_init = true;
    return Status::OK();
}

Status DeltaWriterV2::append(const vectorized::Block* block) {
    return write(block, {}, true);
}

Status DeltaWriterV2::write(const vectorized::Block* block, const std::vector<int>& row_idxs,
                          bool is_append) {
    if (UNLIKELY(row_idxs.empty() && !is_append)) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    if (_is_closed) {
        LOG(WARNING) << "write block after closed tablet_id=" << _req.tablet_id
                     << " load_id=" << _req.load_id << " txn_id=" << _req.txn_id;
        return Status::Error<ALREADY_CLOSED>();
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

Status DeltaWriterV2::_flush_memtable_async() {
    if (_mem_table->empty()) {
        return Status::OK();
    }
    _mem_table->assign_segment_id();
    return _flush_token->submit(std::move(_mem_table));
}

Status DeltaWriterV2::flush_memtable_and_wait(bool need_wait) {
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

Status DeltaWriterV2::wait_flush() {
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

void DeltaWriterV2::_reset_mem_table() {
#ifndef BE_TEST
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num, _load_id.to_string()),
            ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker());
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++, _load_id.to_string()),
            ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker());
#else
    auto mem_table_insert_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableManualInsert:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num, _load_id.to_string()));
    auto mem_table_flush_tracker = std::make_shared<MemTracker>(
            fmt::format("MemTableHookFlush:TabletId={}:MemTableNum={}#loadID={}",
                        std::to_string(tablet_id()), _mem_table_num++, _load_id.to_string()));
#endif
    {
        std::lock_guard<SpinLock> l(_mem_table_tracker_lock);
        _mem_table_insert_trackers.push_back(mem_table_insert_tracker);
        _mem_table_flush_trackers.push_back(mem_table_flush_tracker);
    }
    auto mow_context = std::make_shared<MowContext>(_cur_max_version, _req.txn_id, _rowset_ids,
                                                    _delete_bitmap);
    _mem_table.reset(new MemTable(_tablet, _schema.get(), _tablet_schema.get(), _req.slots,
                                  _req.tuple_desc, _rowset_writer.get(), mow_context,
                                  mem_table_insert_tracker, mem_table_flush_tracker));

    COUNTER_UPDATE(_segment_num, 1);
    _mem_table->set_callback([this](MemTableStat& stat) {
        _memtable_stat += stat;
        COUNTER_SET(_sort_timer, _memtable_stat.sort_ns);
        COUNTER_SET(_agg_timer, _memtable_stat.agg_ns);
        COUNTER_SET(_memtable_duration_timer, _memtable_stat.duration_ns);
        COUNTER_SET(_segment_writer_timer, _memtable_stat.segment_writer_ns);
        COUNTER_SET(_delete_bitmap_timer, _memtable_stat.delete_bitmap_ns);
        COUNTER_SET(_put_into_output_timer, _memtable_stat.put_into_output_ns);
        COUNTER_SET(_sort_times, _memtable_stat.sort_times);
        COUNTER_SET(_agg_times, _memtable_stat.agg_times);
        COUNTER_SET(_raw_rows_num, _memtable_stat.raw_rows);
        COUNTER_SET(_merged_rows_num, _memtable_stat.merged_rows);
    });
}

Status DeltaWriterV2::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriterV2, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }

    if (_is_cancelled) {
        return _cancel_status;
    }

    if (_is_closed) {
        LOG(WARNING) << "close after closed tablet_id=" << _req.tablet_id
                     << " load_id=" << _req.load_id << " txn_id=" << _req.txn_id;
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

Status DeltaWriterV2::close_wait() {
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
        LOG(WARNING) << "previous flush failed tablet " << _tablet->tablet_id();
        return st;
    }

    _mem_table.reset();

    if (_rowset_writer->num_rows() + _memtable_stat.merged_rows != _total_received_rows) {
        LOG(WARNING) << "the rows number written doesn't match, rowset num rows written to file: "
                     << _rowset_writer->num_rows()
                     << ", merged_rows: " << _memtable_stat.merged_rows
                     << ", total received rows: " << _total_received_rows;
        return Status::InternalError("rows number written by delta writer dosen't match");
    }
    {
        SCOPED_TIMER(_rowset_build_timer);
        // use rowset meta manager to save meta
        _cur_rowset = _rowset_writer->build();
    }
    if (_cur_rowset == nullptr) {
        LOG(WARNING) << "fail to build rowset";
        return Status::Error<MEM_ALLOC_FAILED>();
    }
    if (config::experimental_olap_table_sink_v2) {
        RETURN_IF_ERROR(_notify_last_segment());
    }

    if (_tablet->enable_unique_key_merge_on_write()) {
        auto beta_rowset = reinterpret_cast<BetaRowset*>(_cur_rowset.get());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
        // tablet is under alter process. The delete bitmap will be calculated after conversion.
        if (_tablet->tablet_state() == TABLET_NOTREADY &&
            SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
            return Status::OK();
        }
        if (segments.size() > 1) {
            // calculate delete bitmap between segments
            RETURN_IF_ERROR(_tablet->calc_delete_bitmap_between_segments(_cur_rowset, segments,
                                                                         _delete_bitmap));
        }
        RETURN_IF_ERROR(_tablet->commit_phase_update_delete_bitmap(
                _cur_rowset, _rowset_ids, _delete_bitmap, segments, _req.txn_id,
                _rowset_writer.get()));
    }

    Status res;
    {
        SCOPED_TIMER(_commit_txn_timer);
        res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id,
                                                         _req.load_id, _cur_rowset, false);
    }

    if (!res && !res.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id
                     << " for rowset: " << _cur_rowset->rowset_id();
        return res;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        _storage_engine->txn_manager()->set_txn_related_delete_bitmap(
                _req.partition_id, _req.txn_id, _tablet->tablet_id(), _tablet->schema_hash(),
                _tablet->tablet_uid(), true, _delete_bitmap, _rowset_ids);
    }

    _delta_written_success = true;

    // const FlushStatistic& stat = _flush_token->get_stats();
    // print slow log if wait more than 1s
    /*if (_wait_flush_timer->elapsed_time() > 1000UL * 1000 * 1000) {
        LOG(INFO) << "close delta writer for tablet: " << _tablet->tablet_id()
                  << ", load id: " << print_id(_req.load_id) << ", wait close for "
                  << _wait_flush_timer->elapsed_time() << "(ns), stats: " << stat;
    }*/

    COUNTER_UPDATE(_lock_timer, _lock_watch.elapsed_time() / 1000);
    return Status::OK();
}

Status DeltaWriterV2::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriterV2::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    if (_rowset_writer && _rowset_writer->is_doing_segcompaction()) {
        _rowset_writer->wait_flying_segcompaction(); /* already cancel, ignore the return status */
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

int64_t DeltaWriterV2::mem_consumption(MemType mem) {
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

int64_t DeltaWriterV2::active_memtable_mem_consumption() {
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

int64_t DeltaWriterV2::partition_id() const {
    return _req.partition_id;
}

void DeltaWriterV2::_build_current_tablet_schema(int64_t index_id,
                                               const OlapTableSchemaParam* table_schema_param,
                                               const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }

    if (indexes.size() > 0 && indexes[i]->columns.size() != 0 &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(index_id, table_schema_param->version(),
                                                    indexes[i], ori_tablet_schema);
    }
    if (_tablet_schema->schema_version() > ori_tablet_schema.schema_version()) {
        _tablet->update_max_version_schema(_tablet_schema);
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    // set partial update columns info
    _tablet_schema->set_partial_update_info(table_schema_param->is_partial_update(),
                                            table_schema_param->partial_update_input_columns());
}

Status DeltaWriterV2::_notify_last_segment() {
    DCHECK(config::experimental_olap_table_sink_v2);

    brpc::StreamId stream = *_streams.begin();
    RowsetMetaPB rowset_meta_pb = _cur_rowset->rowset_meta()->get_rowset_pb();

    butil::IOBuf buf;
    PStreamHeader header;
    header.set_allocated_load_id(&_req.load_id);
    header.set_index_id(_req.index_id);
    header.set_tablet_id(_req.tablet_id);
    header.set_rowset_id(_cur_rowset->rowset_id().to_string());
    header.set_opcode(doris::PStreamHeader::CLOSE_STREAM);
    header.set_allocated_rowset_meta(&rowset_meta_pb);
    size_t header_len = header.ByteSizeLong();
    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());

    io::StreamSinkFileWriter::send_with_retry(stream, buf);

    header.release_load_id();
    header.release_rowset_meta();
    return Status::OK();
}

} // namespace doris
