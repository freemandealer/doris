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

#include "sink_stream_mgr.h"
#include "util/uid_util.h"
#include "common/config.h"
#include <runtime/exec_env.h>
#include <olap/storage_engine.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/tablet_manager.h>

namespace doris {

bool TargetSegmentComparator::operator()(const TargetSegmentPtr& lhs, const TargetSegmentPtr& rhs) const {
    TargetRowsetComparator rowset_cmp;
    auto less = rowset_cmp.operator()(lhs->target_rowset, rhs->target_rowset);
    auto greater = rowset_cmp.operator()(rhs->target_rowset, lhs->target_rowset);
    // if rowset not equal
    if (less || greater) {
        return less;
    }
    if (lhs->segmentid != rhs->segmentid) {
        return lhs->segmentid < rhs->segmentid;
    }
    return false;
}

bool TargetRowsetComparator::operator()(const TargetRowsetPtr& lhs, const TargetRowsetPtr& rhs) const {
    if (lhs->loadid.hi != rhs->loadid.hi) {
        return lhs->loadid.hi < rhs->loadid.hi;
    }
    if (lhs->loadid.lo != rhs->loadid.lo) {
        return lhs->loadid.lo < rhs->loadid.lo;
    }
    if (lhs->indexid != rhs->indexid) {
        return lhs->indexid < rhs->indexid;
    }
    if (lhs->tabletid != rhs->tabletid) {
        return lhs->tabletid < rhs->tabletid;
    }
    return false;
}

std::string TargetRowset::to_string() {
    std::stringstream ss;
    ss << "loadid: " << loadid << ", indexid: " << indexid << ", tabletid: " << tabletid;
    return ss.str();
}

std::string TargetSegment::to_string() {
    std::stringstream ss;
    ss << target_rowset->to_string() << ", segmentid: " << segmentid;
    return ss.str();
}

SinkStreamHandler::SinkStreamHandler() {
    ThreadPoolBuilder("SinkStreamHandler")
            .set_min_threads(20) // TODO: make them configurable
            .set_max_threads(20)
            .build(&_workers);
}

SinkStreamHandler::~SinkStreamHandler() {
    if (_workers) {
        _workers->shutdown();
    }
}

Status SinkStreamHandler::_create_and_open_file(TargetSegmentPtr target_segment, std::string path) {
    LOG(INFO) << "create and open file, target_segment = " << target_segment->to_string()
              << ", path = " << path;
    std::shared_ptr<std::ofstream> file = std::make_shared<std::ofstream>();
    file->open(path.c_str(), std::ios::out | std::ios::app);
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        _file_map[target_segment] = file; // TODO: better not so global
    }
    return Status::OK();
}

Status SinkStreamHandler::_append_data(TargetSegmentPtr target_segment, std::shared_ptr<butil::IOBuf> message) {
    LOG(INFO) << "append data, target_segment = " << target_segment->to_string()
              << ", data length = " << message->length();
    auto itr = _file_map.end();
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        itr = _file_map.find(target_segment);
    }
    if (itr == _file_map.end()) {
        return Status::InternalError("file not found");
    }
    *(itr->second) << message->to_string();
    return Status::OK();
}

Status SinkStreamHandler::_close_file(TargetSegmentPtr target_segment, bool is_last_segment) {
    LOG(INFO) << "close file, target_segment = " << target_segment->to_string()
              << ", is last segment = " << is_last_segment;
    std::shared_ptr<std::ofstream> file = nullptr;
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        auto itr = _file_map.find(target_segment);
        if (itr == _file_map.end()) {
            return Status::InternalError("close file error");
        }
        file = itr->second;
        _file_map.erase(itr);
    }
    file->close();
    LOG(INFO) << "OOXXOO close file, is_last_segment = " << is_last_segment << " ";
    return Status::OK();
}

void SinkStreamHandler::_report_status(StreamId stream, TargetRowsetPtr target_rowset, bool is_success, std::string error_msg) {
    LOG(INFO) << "OOXXOO report status " << is_success << " " << error_msg;
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    response.set_success(is_success);
    response.set_error_msg(error_msg);
    response.set_index_id(target_rowset->indexid);
    response.set_tablet_id(target_rowset->tabletid);
    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
    if (ret == EAGAIN) {
        LOG(WARNING) << "OOXXOO report status EAGAIN";
    } else if (ret == EINVAL) {
        LOG(WARNING) << "OOXXOO report status EINVAL";
    } else {
        LOG(INFO) << "OOXXOO report status " << ret;
    }
}

void SinkStreamHandler::_parse_header(butil::IOBuf *const message, PStreamHeader& hdr) {
    butil::IOBufAsZeroCopyInputStream wrapper(*message);
    hdr.ParseFromZeroCopyStream(&wrapper);
    // TODO: make it VLOG
    LOG(INFO) << "header parse result:"
              << "opcode = " << hdr.opcode()
              << ", loadid = " << hdr.load_id()
              << ", indexid = " << hdr.index_id()
              << ", tabletid = " << hdr.tablet_id()
              << ", segmentid = " << hdr.segment_id()
              << ", is_last_segment = " << (hdr.has_is_last_segment()?hdr.is_last_segment():false);
}

uint64_t SinkStreamHandler::get_next_segmentid(TargetRowsetPtr target_rowset, int64_t segmentid, bool is_open) {
    // TODO: need support concurrent flush memtable
    {
        std::lock_guard<std::mutex> l(_tablet_segment_next_id_lock);
        if (_tablet_segment_next_id.find(target_rowset) == _tablet_segment_next_id.end()) {
            _tablet_segment_next_id[target_rowset] = 0;
            return 0;
        }
        if (is_open) {
            return _tablet_segment_next_id[target_rowset]++;
        } else {
            return _tablet_segment_next_id[target_rowset];
        }
    }
}

Status SinkStreamHandler::_build_rowset(TargetRowsetPtr target_rowset, const RowsetMetaPB& rowset_meta_pb) {
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(  //TODO
            rowset_meta_pb.tablet_id(), rowset_meta_pb.tablet_schema_hash());
    std::string rowset_meta_str;
    bool ret = rowset_meta_pb.SerializeToString(&rowset_meta_str);
    if (!ret) {
        LOG(WARNING) << "failed to parse rowset meta pb sent by sink "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to parse rowset meta pb sent by sink");
    }

    bool parsed = rowset_meta->init(rowset_meta_str);
    RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();
    rowset_meta->set_rowset_id(new_rowset_id);
    rowset_meta->set_tablet_uid(tablet->tablet_uid());
    if (!parsed) {
        LOG(WARNING) << "failed to init rowset meta "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to init rowset meta");
    }

    RowsetSharedPtr rowset;
    Status create_status = RowsetFactory::create_rowset(
            tablet->tablet_schema(), tablet->tablet_path(), rowset_meta, &rowset);
    if (!create_status) {
        LOG(WARNING) << "failed to create rowset "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to create rowset");
    }

    return Status::OK();
}

void SinkStreamHandler::_handle_message(StreamId stream, PStreamHeader hdr,
                                        TargetRowsetPtr target_rowset,
                                        TargetSegmentPtr target_segment,
                                        std::shared_ptr<butil::IOBuf> message) {
    Status s = Status::OK();
    std::string path;
    switch(hdr.opcode()) {
    case PStreamHeader::OPEN_FILE:
        path = message->to_string();
        s = _create_and_open_file(target_segment, path);
        break;
    case PStreamHeader::APPEND_DATA:
        s= _append_data(target_segment, message);
        break;
    case PStreamHeader::CLOSE_FILE:
        s = _close_file(target_segment, hdr.is_last_segment());
        if (hdr.has_is_last_segment() && hdr.is_last_segment()) {
            DCHECK(hdr.has_rowset_meta());
            s = _build_rowset(target_rowset, hdr.rowset_meta());
            if (s.ok()) {
                return _report_status(stream, target_rowset, true, s.to_string());
            }
        }
        break;
    default:
        DCHECK(false);
    }
    if (!s.ok()) {
        LOG(WARNING) << "Failed to handle " << PStreamHeader_Opcode_Name(hdr.opcode())
                     << " message in stream (" << stream << "), target segment ("
                     << target_segment->to_string() << "), reason: " << s.to_string();
        _report_status(stream, target_rowset, false, s.to_string());
    }
}

//TODO trigger build meta when last segment of all cluster is closed
int SinkStreamHandler::on_received_messages(StreamId id, butil::IOBuf *const messages[], size_t size) {
    LOG(INFO) << "OOXXOO on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        std::shared_ptr<butil::IOBuf> messageBuf = std::make_shared<butil::IOBuf>(messages[i]->movable()); // hold the data
        size_t hdr_len = 0;
        messageBuf->cutn((void*)&hdr_len, sizeof(size_t));
        butil::IOBuf hdr_buf;
        PStreamHeader hdr;
        messageBuf->cutn(&hdr_buf, hdr_len);
        _parse_header(&hdr_buf, hdr);

        TargetRowsetPtr target_rowset = std::make_shared<TargetRowset>();
        target_rowset->loadid = hdr.load_id();
        target_rowset->indexid = hdr.index_id();
        target_rowset->tabletid = hdr.tablet_id();
        uint64_t final_segmentid = get_next_segmentid(target_rowset, hdr.segment_id(), hdr.opcode() == PStreamHeader::OPEN_FILE);
        TargetSegmentPtr target_segment = std::make_shared<TargetSegment>();
        target_segment->target_rowset = target_rowset;
        target_segment->segmentid = final_segmentid;

        // serialize OPs on same file: open, write1, write2, ... , close
        if (_segment_token_map.find(target_segment) == _segment_token_map.end()) {
            _segment_token_map[target_segment] = _workers->new_token(ThreadPool::ExecutionMode::SERIAL);
        }

        auto token = _segment_token_map[target_segment];
        token->submit_func([this, id, hdr, target_rowset, target_segment, messageBuf]() {
            _handle_message(id, hdr, target_rowset, target_segment, messageBuf);
        });
        LOG(INFO) << "OOXXOO target_segment: " << target_segment->to_string()
                  << " submitted to threadpool via token: " << token.get();
    }
    return 0;
}

void SinkStreamHandler::on_idle_timeout(StreamId id) {

}

void SinkStreamHandler::on_closed(StreamId id) {
    auto env = doris::ExecEnv::GetInstance();
    StreamIdPtr id_ptr = std::make_shared<StreamId>(id);
    env->get_sink_stream_mgr()->release_stream_id(id_ptr);
}

SinkStreamMgr::SinkStreamMgr() {
    for (int i = 0; i < 1000; ++i) {
        StreamIdPtr stream_id = std::make_shared<StreamId>();
        _free_stream_ids.push_back(stream_id);
    }
    _handler = std::make_shared<SinkStreamHandler>();
}

SinkStreamMgr::~SinkStreamMgr() {
    for (auto& ptr : _free_stream_ids) {
        ptr.reset();
    }
}

StreamIdPtr SinkStreamMgr::get_free_stream_id() {
    StreamIdPtr ptr = nullptr;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_free_stream_ids.empty()) {
            ptr = std::make_shared<StreamId>();
        } else {
            ptr = _free_stream_ids.back();
            _free_stream_ids.pop_back();
        }
    }
    return ptr;
}

void SinkStreamMgr::release_stream_id(StreamIdPtr id) {
    {
        std::lock_guard<std::mutex> l(_lock);
        _free_stream_ids.push_back(id);
    }
}

} // namespace doris
