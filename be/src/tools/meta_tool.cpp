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

#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_utils.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/olap_common.h"
#include "olap/fs/block_manager.h"
#include "olap/short_key_index.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "common/config.h"
#include "olap/options.h"
#include "olap/storage_engine.h"

using std::filesystem::path;
using doris::DataDir;
using doris::OLAP_SUCCESS;
using doris::OlapMeta;
using doris::OLAPStatus;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;
using doris::FileUtils;
using doris::Slice;
using doris::RandomAccessFile;
using strings::Substitute;
using doris::segment_v2::SegmentFooterPB;
using doris::segment_v2::ColumnReader;
using doris::segment_v2::BinaryPlainPageDecoder;
using doris::segment_v2::PageHandle;
using doris::segment_v2::PagePointer;
using doris::segment_v2::ColumnReaderOptions;
using doris::segment_v2::ColumnIteratorOptions;
using doris::segment_v2::PageFooterPB;

using namespace doris;

const std::string HEADER_PREFIX = "tabletmeta_";

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, show_meta");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path "
          "--tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path "
          "--json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid "
          "--schema_hash=schemahash\n";
    ss << "./meta_tool --operation=delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    OLAPStatus s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (s != OLAP_SUCCESS) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    doris::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    OLAPStatus s =
            TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
    if (s == doris::OLAP_ERR_META_KEY_NOT_FOUND) {
        std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        return;
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    OLAPStatus s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (s != OLAP_SUCCESS) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    OLAPStatus s = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
    if (s != OLAP_SUCCESS) {
        std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete meta successfully" << std::endl;
}

Status init_data_dir(const std::string& dir, std::unique_ptr<DataDir>* ret) {
    std::string root_path;
    Status st = FileUtils::canonicalize(dir, &root_path);
    if (!st.ok()) {
        std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string()
                  << std::endl;
        return Status::InternalError("invalid root path");
    }
    doris::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (res != OLAP_SUCCESS) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return Status::InternalError("parse root path failed");
    }

    std::unique_ptr<DataDir> p(
            new (std::nothrow) DataDir(path.path, path.capacity_bytes, path.storage_medium, path.remote_path));
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    st = p->init();
    if (!st.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/palo.HDD,100010,11212389324
    //      /data2/palo.HDD,100010,23049230234
    std::ifstream infile(tablet_file);
    std::string line = "";
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        std::vector<string> v = strings::Split(line, ",");
        if (v.size() != 3) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = FileUtils::canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            Status st = init_data_dir(dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path
                          << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!safe_strto64(v[1].c_str(), &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        int64_t schema_hash;
        if (!safe_strto64(v[2].c_str(), &schema_hash)) {
            std::cout << "invalid schema hash: " << line << std::endl;
            err_num++;
            continue;
        }

        OLAPStatus s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
        if (s != OLAP_SUCCESS) {
            std::cout << "delete tablet meta failed for tablet_id:" << tablet_id
                      << ", schema_hash:" << schema_hash << ", status:" << s << std::endl;
            err_num++;
            continue;
        }

        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num
              << std::endl;
    return;
}

Status get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string file_name = input_file->file_name();
    uint64_t file_size;
    RETURN_IF_ERROR(input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12",
                                                      file_name, file_size));
    }

    uint8_t fixed_buf[12];
    Slice slice(fixed_buf, 12);
    RETURN_IF_ERROR(input_file->read_at(file_size - 12, &slice));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = doris::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2",
                                                      file_name, file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    Slice slice2(footer_buf);
    RETURN_IF_ERROR(input_file->read_at(file_size - 12 - footer_length, &slice2));

    // validate footer PB's checksum
    uint32_t expect_checksum = doris::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = doris::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2", file_name,
                actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(strings::Substitute(
                "Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

void show_segment_footer(const std::string& file_name) {
    std::unique_ptr<RandomAccessFile> input_file;
    Status status = doris::Env::Default()->new_random_access_file(file_name, &input_file);
    if (!status.ok()) {
        std::cout << "open file failed: " << status.to_string() << std::endl;
        return;
    }
    SegmentFooterPB footer;
    status = get_segment_footer(input_file.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
    return;
}

StorageEngine* k_engine = nullptr;

void SetUp() {
    config::tablet_map_shard_size = 1;
    config::txn_map_shard_size = 1;
    config::txn_shard_size = 1;
    config::default_rowset_type = "BETA";
    config::disable_storage_page_cache = true;

    char buffer[128];
    ignore_result(getcwd(buffer, 128));
    config::storage_root_path = std::string(buffer) + "/data_test";

    FileUtils::remove_all(config::storage_root_path).ok();
    FileUtils::create_dir(config::storage_root_path).ok();

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    Status s = doris::StorageEngine::open(options, &k_engine);
    (void)s;
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_storage_engine(k_engine);

    const std::string rowset_dir = "./data_test/data/beta_rowset_test";

}

void check_data(const std::string& file_name) {
    SetUp();
    FilePathDesc path_desc(file_name);
    std::unique_ptr<RandomAccessFile> input_file;
    Status status = doris::Env::Default()->new_random_access_file(file_name, &input_file);
    if (!status.ok()) {
        std::cout << "open file failed: " << status.to_string() << std::endl;
        return;
    }

    // get footer
    SegmentFooterPB* footer = new SegmentFooterPB();
    status = get_segment_footer(input_file.get(), footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }

    size_t column_num = footer->columns_size();
    for (int i = 0; i < column_num; ++i) {
        std::cout << "begin of a column" << std::endl;
        const ColumnMetaPB& column_meta = footer->columns(i);

        std::string json_footer;
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        bool ret = json2pb::ProtoMessageToJson(column_meta, &json_footer, json_options);
        if (!ret) {
            std::cout << "Convert PB to json failed" << std::endl;
            return;
        }
        std::cout << json_footer << std::endl;

        size_t num_rows = column_meta.num_rows();
        std::unique_ptr<ColumnReader> reader;
        ColumnReaderOptions opts;
        ColumnReader::create(opts, column_meta, num_rows, path_desc, &reader);

        OrdinalPageIndexIterator ordinal_page_index_itr;
        reader->seek_to_first(&ordinal_page_index_itr);

        while (ordinal_page_index_itr.valid()) {
            const PagePointer& pp = ordinal_page_index_itr.page();
            std::cout << "begin of a page" << std::endl;


            uint8_t* fixed_buf = (uint8_t*)malloc(pp.size);
            Slice slice(fixed_buf, pp.size);
            input_file->read_at(pp.offset, &slice);

            uint32_t footer_length = decode_fixed32_le((uint8_t*)slice.data + slice.size - 8);
            //uint8_t* pgft = fixed_buf + pp.size - footer_length;
            std::cout << pp.offset << "," << pp.size << "," << footer_length << std::endl;
            {
                PageFooterPB pagefooter;

                std::string footer_buf;
                footer_buf.resize(footer_length);
                Slice slice2(footer_buf);
                input_file->read_at(pp.offset + pp.size - footer_length - 8, &slice2);

                // deserialize footer PB
                if (!pagefooter.ParseFromString(footer_buf)) {
                    std::cout <<"parse failed" << std::endl;
                }

                std::string json_footer;
                json2pb::Pb2JsonOptions json_options;
                json_options.pretty_json = true;
                bool ret = json2pb::ProtoMessageToJson(pagefooter, &json_footer, json_options);
                if (!ret) {
                    std::cout << "Convert PB to json failed" << std::endl;
                    return;
                }
                std::cout << json_footer << std::endl;
            }

            // TODO: try to decompress the page to check length, or even try to decode the page

            std::cout << "end of a page" << std::endl;

            ordinal_page_index_itr.next();
        }
#if 0
        const OrdinalIndexPB* pgpb = reader->ordinal_index_meta();

        if (!pgpb) {
            continue;
        }
        PagePointer pp(pgpb->root_page().root_page().offset(), pgpb->root_page().root_page().size());
#endif
        //FileColumnIterator file_column_itr(reader.get());
#if 0
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = nullptr;
        iter_opts.use_page_cache = false;
        std::unique_ptr<fs::ReadableBlock> rblock;
        config::disable_storage_page_cache = true;

        fs::BlockManager* block_mgr = fs::fs_util::block_manager(TStorageMedium::HDD);
        block_mgr->open_block(path_desc, &rblock);
        iter_opts.rblock = rblock.get();

        // file_column_itr.init(iter_opts);

        std::unique_ptr<BlockCompressionCodec> codec;
        get_block_compression_codec(reader->get_compression(), codec);


        PageHandle handle;
        Slice page_body;
        PageFooterPB page_footer;
        reader->read_page(iter_opts, pp, &handle, &page_body, &page_footer, codec.get());
#endif
#if 0
        OrdinalPageIndexIterator zdata;
        file_column_itr.read_data_page(zdata);

        bool is_eos = false;
        while (file_column_itr.load_next_page(&is_eos)) {
            std::cout << "    load one page" << std::endl;
        }
#endif
        std::cout << "end of a column" << std::endl;
    }


#if 0
    size_t column_num = footer.columns_size();
    for (int i = 0; i < column_num; ++i) {
        // step1. get OrdinalIndex for each column
        ColumnMetaPB column_meta = footer.columns(i);
        size_t row_num = column_meta.num_rows();
        size_t index_num = column_meta.indexes_size();
        std::cout << "index number:" << index_num << std::endl;
        for (int j = 0; j < index_num; ++j) {
            OrdinalIndexPB ordinal_index = column_meta.indexes(j).ordinal_index();
            // step2. parse OrdinalIndex to get data pages
            OrdinalIndexReader reader(path_desc, &ordinal_index, row_num);
            reader.load(false, false);
            for (auto itr = reader.begin(); itr.valid(); itr.next()) {
                // step3. check each data page
                auto pg = itr.page();

#if 0
                // 3.1 decompress and load to memory
                PageReadOptions opts;
                opts.rblock = iter_opts.rblock;
                opts.page_pointer = pp;
                opts.codec = codec;
                opts.stats = iter_opts.stats;
                opts.verify_checksum = _opts.verify_checksum;
                opts.use_page_cache = iter_opts.use_page_cache;
                opts.kept_in_memory = _opts.kept_in_memory;
                opts.type = iter_opts.type;
                opts.encoding_info = _encoding_info;

                PageIO::read_and_decompress_page(opts, handle, page_body, footer);
                // 3.2 parse pagefooter to form parsed page
                // 3.3 decode
#endif

            }
        }
    }
#endif

#if 0
    // get short key index page
    auto sk_index_page = footer.short_key_index_page();
    uint64_t sk_index_page_offset = sk_index_page.offset();
    uint32_t sk_index_page_size = sk_index_page.size();

    // load short key index from page

    uint8_t fixed_buf[sk_index_page_size];
    Slice slice(fixed_buf, sk_index_page_size);
    input_file->read_at(sk_index_page_offset, &slice);
#endif

#if 0
    FilePathDesc path_desc(file_name);
    std::unique_ptr<fs::ReadableBlock> rblock;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(TStorageMedium::SSD);
    block_mgr->open_block(path_desc, &rblock);
    OlapReaderStatistics tmp_stats;

    PageReadOptions opts;
    opts.use_page_cache = false;
    opts.rblock = rblock.get();
    opts.page_pointer = PagePointer(footer.short_key_index_page());
    opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
    opts.stats = &tmp_stats;
    opts.type = INDEX_PAGE;

    PageHandle sk_index_handle;

    Slice body;
    PageFooterPB page_footer;
    PageIO::read_and_decompress_page(opts, &sk_index_handle, &body, &page_footer);
    DCHECK_EQ(page_footer.type(), SHORT_KEY_PAGE);
    DCHECK(page_footer.has_short_key_page_footer());

    auto decoder = new ShortKeyIndexDecoder();
    decoder->parse(body, page_footer.short_key_page_footer());

    std::cout << "short key index num:" << decoder->num_items() << std::endl;
    for (auto itr = decoder->begin(); itr != decoder->end(); ++itr) {
        auto a = *itr;
    }
#endif

#if 0
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
#endif
    return;
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st = FileUtils::canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file
                      << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show_segment_footer" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file);
    } else if (FLAGS_operation == "check_data") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for check_data" << std::endl;
            return -1;
        }
        check_data(FLAGS_file);
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta", "load_meta", "delete_meta"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(FLAGS_root_path, &data_dir);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string()
                      << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
