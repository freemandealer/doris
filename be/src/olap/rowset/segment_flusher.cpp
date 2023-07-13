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

#include "olap/rowset/segment_flusher.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h" // LocalSchemaChangeRecorder
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
using namespace ErrorCode;

Status SegmentFlusher::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    return Status::OK();
}

Status SegmentFlusher::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          SegmentStatistics& segstat, int64_t* flush_size,
                                          TabletSchemaSPtr flush_schema) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    bool no_compression = block->bytes() <= config::segment_compression_threshold_kb * 1024;
    RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression, flush_schema));
    RETURN_IF_ERROR(_add_rows(writer, block, 0, block->rows()));
    RETURN_IF_ERROR(_flush_segment_writer(writer, segstat, flush_size));
    return Status::OK();
}

Status SegmentFlusher::close() {
    std::lock_guard<SpinLock> l(_lock);
    for (auto& file_writer : _file_writers) {
        Status status = file_writer->close();
        if (!status.ok()) {
            LOG(WARNING) << "failed to close file writer, path=" << file_writer->path()
                         << " res=" << status;
            return status;
        }
    }
    return Status::OK();
}

Status SegmentFlusher::_add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                                 const vectorized::Block* block, size_t row_offset,
                                 size_t row_num) {
    auto s = segment_writer->append_block(block, row_offset, row_num);
    if (UNLIKELY(!s.ok())) {
        return Status::Error<WRITER_DATA_WRITE_ERROR>("failed to append block: {}", s.to_string());
    }
    _num_rows_written += row_num;
    return Status::OK();
}

Status SegmentFlusher::_create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                              int32_t segment_id, bool no_compression,
                                              TabletSchemaSPtr flush_schema) {
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, file_writer));

    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;
    if (no_compression) {
        writer_options.compression_type = NO_COMPRESSION;
    }

    const auto& tablet_schema = flush_schema ? flush_schema : _context.tablet_schema;
    writer.reset(new segment_v2::SegmentWriter(
            file_writer.get(), segment_id, tablet_schema, _context.tablet, _context.data_dir,
            _context.max_rows_per_segment, writer_options, _context.mow_context));
    {
        std::lock_guard<SpinLock> l(_lock);
        _file_writers.push_back(std::move(file_writer));
    }
    auto s = writer->init();
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer.reset();
        return s;
    }
    return Status::OK();
}

Status SegmentFlusher::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                             SegmentStatistics& segstat, int64_t* flush_size) {
    uint32_t row_num = writer->num_rows_written();

    if (writer->num_rows_written() == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = writer->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        return Status::Error(s.code(), "failed to finalize segment: {}", s.to_string());
    }
    VLOG_DEBUG << "tablet_id:" << _context.tablet_id
               << " flushing filename: " << writer->get_data_dir()->path()
               << " rowset_id:" << _context.rowset_id;

    KeyBoundsPB key_bounds;
    Slice min_key = writer->min_encoded_key();
    Slice max_key = writer->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    segstat.row_num = row_num;
    segstat.data_size = segment_size + writer->get_inverted_index_file_size();
    segstat.index_size = index_size + writer->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;

    writer.reset();
    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    return Status::OK();
}

Status SegmentFlushWriter::flush(uint32_t& segment_id, SegmentStatistics& segstat) {
    if (_writer == nullptr) {
        segstat.row_num = 0; // means no need to add_segment
        return Status::OK();
    }
    segment_id = _writer->get_segment_id();
    return _flusher->_flush_segment_writer(_writer, segstat);
}

int64_t SegmentFlushWriter::max_row_to_add(size_t row_avg_size_in_bytes) {
    if (_writer == nullptr) {
        return 0;
    }
    return _writer->max_row_to_add(row_avg_size_in_bytes);
}

Status BetaRowsetSegmentWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _segment_flusher.init(rowset_writer_context);
    _segment_collector = rowset_writer_context.segment_collector;
    return Status::OK();
}

Status BetaRowsetSegmentWriter::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    do {
        auto max_row_add = _flush_writer.max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another single row, need flush now
            RETURN_IF_ERROR(flush());
            // TODO: RETURN_IF_ERROR(_check_segment_number_limit());
            RETURN_IF_ERROR(_flush_writer.init(allocate_segment_id()));
            max_row_add = _flush_writer.max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }
        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        RETURN_IF_ERROR(_flush_writer.add_rows(block, row_offset, input_row_num));
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    return Status::OK();
}

Status BetaRowsetSegmentWriter::flush() {
    SegmentStatistics segstats;
    uint32_t segment_id = 0;
    RETURN_IF_ERROR(_flush_writer.flush(segment_id, segstats));
    if (segstats.row_num > 0) {
        RETURN_IF_ERROR(_segment_collector->add(segment_id, segstats));
    }
    return Status::OK();
}

Status BetaRowsetSegmentWriter::flush_single_block(const vectorized::Block* block,
                                                   int32_t segment_id, int64_t* flush_size,
                                                   TabletSchemaSPtr flush_schema) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    //TODO: RETURN_IF_ERROR(_check_segment_number_limit());
    SegmentStatistics segstats;
    RETURN_IF_ERROR(_segment_flusher.flush_single_block(block, segment_id, segstats, flush_size,
                                                        flush_schema));
    RETURN_IF_ERROR(_segment_collector->add(segment_id, segstats));
    return Status::OK();
}

Status BetaRowsetSegmentWriter::close() {
    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_segment_flusher.close());
    return Status::OK();
}

} // namespace doris
