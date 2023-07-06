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

#include "olap/rowset/beta_rowset_writer_v2.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <memory>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/integral_types.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/stream_sink_file_writer.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/common/schema_util.h" // LocalSchemaChangeRecorder
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetWriterV2::BetaRowsetWriterV2(const std::vector<brpc::StreamId>& streams)
        : _next_segment_id(0),
          _num_segment(0),
          _segment_writer(nullptr),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0),
          _raw_num_rows_written(0),
          _streams(streams) {}

BetaRowsetWriterV2::~BetaRowsetWriterV2() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed

        // TODO: send signal to remote to cleanup segments
    }
}

Status BetaRowsetWriterV2::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    return Status::OK();
}

Status BetaRowsetWriterV2::_do_add_block(const vectorized::Block* block,
                                         std::unique_ptr<segment_v2::SegmentWriter>* segment_writer,
                                         size_t row_offset, size_t input_row_num) {
    auto s = (*segment_writer)->append_block(block, row_offset, input_row_num);
    if (UNLIKELY(!s.ok())) {
        LOG(WARNING) << "failed to append block: " << s.to_string();
        return Status::Error<WRITER_DATA_WRITE_ERROR>();
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::_add_block(const vectorized::Block* block,
                                      std::unique_ptr<segment_v2::SegmentWriter>* segment_writer,
                                      const FlushContext* flush_ctx) {
    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    if (flush_ctx != nullptr && flush_ctx->segment_id.has_value()) {
        // the entire block (memtable) should be flushed into single segment
        RETURN_IF_ERROR(_do_add_block(block, segment_writer, 0, block_row_num));
        _raw_num_rows_written += block_row_num;
        return Status::OK();
    }

    do {
        auto max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another single row, need flush now
            RETURN_IF_ERROR(_flush_segment_writer(segment_writer));
            RETURN_IF_ERROR(_create_segment_writer(segment_writer, flush_ctx));
            max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }
        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        RETURN_IF_ERROR(_do_add_block(block, segment_writer, row_offset, input_row_num));
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    _raw_num_rows_written += block_row_num;
    return Status::OK();
}

Status BetaRowsetWriterV2::flush() {
    if (_segment_writer != nullptr) {
        RETURN_IF_ERROR(_flush_segment_writer(&_segment_writer));
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_single_memtable(const vectorized::Block* block, int64* flush_size,
                                                 const FlushContext* ctx) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    std::unique_ptr<segment_v2::SegmentWriter> writer;
    RETURN_IF_ERROR(_create_segment_writer(&writer, ctx));
    segment_v2::SegmentWriter* raw_writer = writer.get();
    int32_t segment_id = writer->get_segment_id();
    RETURN_IF_ERROR(_add_block(block, &writer, ctx));
    // if segment_id is present in flush context,
    // the entire memtable should be flushed into a single segment
    if (ctx != nullptr && ctx->segment_id.has_value()) {
        DCHECK_EQ(writer->get_segment_id(), segment_id);
        DCHECK_EQ(writer.get(), raw_writer);
    }
    RETURN_IF_ERROR(_flush_segment_writer(&writer, flush_size));
    return Status::OK();
}

Status BetaRowsetWriterV2::_do_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, int64_t begin, int64_t end,
        const FlushContext* flush_ctx) {
    int32_t segment_id = (flush_ctx != nullptr && flush_ctx->segment_id.has_value())
                                 ? flush_ctx->segment_id.value()
                                 : allocate_segment_id();
    io::FileWriterPtr file_writer;
    auto partition_id = _context.partition_id;
    auto sender_id = _context.sender_id;
    auto index_id = _context.index_id;
    auto tablet_id = _context.tablet_id;
    auto load_id = _context.load_id;
    auto stream_id = *_streams.begin();

    auto stream_writer = std::make_unique<io::StreamSinkFileWriter>(sender_id, stream_id);
    stream_writer->init(load_id, partition_id, index_id, tablet_id, segment_id);
    file_writer = std::move(stream_writer);

    DCHECK(file_writer != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;

    writer->reset(new segment_v2::SegmentWriter(file_writer.get(), segment_id,
                                                _context.tablet_schema, _context.tablet,
                                                _context.data_dir, _context.max_rows_per_segment,
                                                writer_options, _context.mow_context));
    (*writer)->set_use_stream_sink_file_writer();

    {
        std::lock_guard<SpinLock> l(_lock);
        _file_writers.push_back(std::move(file_writer));
    }
    auto s = (*writer)->init(flush_ctx);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }

    return Status::OK();
}

Status BetaRowsetWriterV2::_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, const FlushContext* flush_ctx) {
    size_t total_segment_num = _num_segment;
    if (UNLIKELY(total_segment_num > config::max_segment_num_per_rowset)) {
        LOG(WARNING) << "too many segments in rowset."
                     << " tablet_id:" << _context.tablet_id << " rowset_id:" << _context.rowset_id
                     << " max:" << config::max_segment_num_per_rowset
                     << " _num_segment:" << _num_segment;
        return Status::Error<TOO_MANY_SEGMENTS>();
    } else {
        return _do_create_segment_writer(writer, -1, -1, flush_ctx);
    }
}

Status BetaRowsetWriterV2::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer,
                                                 int64_t* flush_size) {
    uint32_t segid = (*writer)->get_segment_id();
    uint32_t row_num = (*writer)->num_rows_written();

    if ((*writer)->num_rows_written() == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = (*writer)->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        LOG(WARNING) << "failed to finalize segment: " << s.to_string();
        return Status::Error<WRITER_DATA_WRITE_ERROR>();
    }
    VLOG_DEBUG << "tablet_id:" << _context.tablet_id
               << " flushing filename: " << (*writer)->get_data_dir()->path()
               << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment;

    KeyBoundsPB key_bounds;
    Slice min_key = (*writer)->min_encoded_key();
    Slice max_key = (*writer)->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    Statistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + (*writer)->get_inverted_index_file_size();
    segstat.index_size = index_size + (*writer)->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segid) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segid, segstat);
        _segment_num_rows.resize(_next_segment_id);
        _segment_num_rows[segid] = row_num;
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segid:" << segid << " row_num:" << row_num
               << " data_size:" << segment_size << " index_size:" << index_size;

    writer->reset();
    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    {
        std::lock_guard<std::mutex> lock(_segment_set_mutex);
        _segment_set.add(segid);
        while (_segment_set.contains(_num_segment)) {
            _num_segment++;
        }
    }
    return Status::OK();
}

} // namespace doris
