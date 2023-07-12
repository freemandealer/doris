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

#pragma once

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "segcompaction.h"
#include "segment_v2/segment.h"
#include "util/spinlock.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

struct SegmentStatistics;
class FlushWriter;
class BetaRowsetWriter;

class SegmentFlusher {
    friend class FlushWriter;

public:
    SegmentFlusher() = default;

    ~SegmentFlusher() = default;

    Status init(const RowsetWriterContext& rowset_writer_context);

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              SegmentStatistics& segstat, int64_t* flush_size = nullptr,
                              TabletSchemaSPtr flush_schema = nullptr);

    std::unique_ptr<FlushWriter> create_writer();

    int64_t num_rows() const { return _num_rows_written; }

    Status close();

private:
    Status _add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                     const vectorized::Block* block, size_t row_offset, size_t row_num);
    Status _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                  int32_t segment_id, bool no_compression = false,
                                  TabletSchemaSPtr flush_schema = nullptr);
    Status _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                 SegmentStatistics& segstat, int64_t* flush_size = nullptr);

private:
    RowsetWriterContext _context;

    mutable SpinLock _lock; // protect following vectors.
    std::vector<io::FileWriterPtr> _file_writers;

    // written rows by add_block/add_row
    std::atomic<int64_t> _num_rows_written = 0;
};

class FlushWriter {
public:
    FlushWriter(SegmentFlusher* flusher) : _flusher(flusher) {}

    ~FlushWriter() = default;

    Status init(uint32_t segment_id) {
        return _flusher->_create_segment_writer(_writer, segment_id);
    }

    Status add_rows(const vectorized::Block* block, size_t row_offset, size_t input_row_num) {
        return _flusher->_add_rows(_writer, block, row_offset, input_row_num);
    }

    Status flush(uint32_t& segment_id, SegmentStatistics& segstat);

    int64_t max_row_to_add(size_t row_avg_size_in_bytes);

private:
    SegmentFlusher* _flusher;
    std::unique_ptr<segment_v2::SegmentWriter> _writer;
};

class BetaRowsetSegmentWriter {
public:
    BetaRowsetSegmentWriter(std::function<void(uint32_t, SegmentStatistics&)> add_segment)
            : _add_segment(add_segment) {}

    ~BetaRowsetSegmentWriter() = default;

    Status init(const RowsetWriterContext& rowset_writer_context);

    void set_segment_start_id(uint32_t start_id) { _next_segment_id = start_id; }

    Status add_block(const vectorized::Block* block);

    Status flush();

    int32_t allocate_segment_id() { return _next_segment_id.fetch_add(1); }

    int32_t next_segment_id() const { return _next_segment_id.load(); }

    int64_t num_rows() const { return _segment_flusher.num_rows(); }

    // Flush a block into a single segment, with pre-allocated segment_id.
    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              int64_t* flush_size = nullptr,
                              TabletSchemaSPtr flush_schema = nullptr);

    // Flush a block into a single segment, without pre-allocated segment_id.
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block) {
        return flush_single_block(block, allocate_segment_id());
    }

    Status close();

private:
    std::function<void(uint32_t, SegmentStatistics&)> _add_segment;

    std::atomic<int32_t> _next_segment_id = 0;
    SegmentFlusher _segment_flusher;
    std::unique_ptr<FlushWriter> _segment_writer;
};

} // namespace doris
