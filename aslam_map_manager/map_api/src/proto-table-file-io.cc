#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <map-api/chunk-manager.h>
#include <map-api/cr-table.h>
#include <map-api/cru-table.h>
#include <map-api/proto-table-file-io.h>
#include <map-api/transaction.h>

namespace map_api {
ProtoTableFileIO::ProtoTableFileIO(const std::string& filename,
                                   map_api::NetTable* table)
    : file_name_(filename), table_(CHECK_NOTNULL(table)) {
  file_.open(filename,
             std::fstream::binary | std::ios_base::in | std::ios_base::out);
  if (!file_.is_open()) {
    file_.open(filename, std::fstream::binary | std::fstream::in |
                             std::fstream::out | std::fstream::trunc);
  }
  CHECK(file_.is_open()) << "Couldn't open file " << filename;
}

ProtoTableFileIO::~ProtoTableFileIO() {}

bool ProtoTableFileIO::StoreTableContents(const map_api::LogicalTime& time) {
  map_api::Transaction transaction(time);
  map_api::CRTable::RevisionMap revisions =
      transaction.dumpActiveChunks(table_);

  CHECK(file_.is_open());

  for (const std::pair<Id, std::shared_ptr<Revision> >& pair : revisions) {
    CHECK(pair.second != nullptr);
    const Revision& revision = *pair.second;

    RevisionStamp current_item_stamp;
    CHECK(revision.get(CRTable::kIdField, &current_item_stamp.first));
    CHECK_EQ(current_item_stamp.first, pair.first);

    if (table_->type() == CRTable::Type::CRU) {
      CHECK(
          revision.get(CRUTable::kUpdateTimeField, &current_item_stamp.second));
    } else {
      CHECK(
          revision.get(CRTable::kInsertTimeField, &current_item_stamp.second));
    }

    // Look up if we already stored this item, if we have this revision skip.
    bool already_stored = already_stored_items_.count(current_item_stamp) > 0;
    if (!already_stored) {
      // Moving read to the beginning of the file.
      file_.clear();
      file_.seekg(0);
      if (file_.peek() == std::char_traits<char>::eof()) {
        file_.clear();
        file_.seekp(0);
        google::protobuf::io::OstreamOutputStream raw_out(&file_);
        google::protobuf::io::CodedOutputStream coded_out(&raw_out);
        coded_out.WriteLittleEndian32(1);
      } else {
        file_.clear();
        file_.seekg(0);
        // Only creating these once we know the file isn't empty.
        google::protobuf::io::IstreamInputStream raw_in(&file_);
        google::protobuf::io::CodedInputStream coded_in(&raw_in);
        uint32_t message_count;
        coded_in.ReadLittleEndian32(&message_count);

        ++message_count;

        file_.clear();
        file_.seekp(0);
        google::protobuf::io::OstreamOutputStream raw_out(&file_);
        google::protobuf::io::CodedOutputStream coded_out(&raw_out);
        coded_out.WriteLittleEndian32(message_count);
      }

      // Go to end of file and write message size and then the message.
      file_.clear();
      file_.seekp(0, std::ios_base::end);

      google::protobuf::io::OstreamOutputStream raw_out(&file_);
      google::protobuf::io::CodedOutputStream coded_out(&raw_out);

      coded_out.WriteVarint32(revision.ByteSize());
      revision.SerializeToCodedStream(&coded_out);
      already_stored_items_.insert(current_item_stamp);
    }
  }
  return true;
}

bool ProtoTableFileIO::RestoreTableContents() {
  CHECK(file_.is_open());

  file_.clear();
  file_.seekg(0, std::ios::end);
  std::istream::pos_type file_size = file_.tellg();

  if (file_size == 0) {
    LOG(ERROR) << "Got file of size: " << file_size;
    return false;
  }

  file_.clear();
  file_.seekg(0, std::ios::beg);
  google::protobuf::io::IstreamInputStream raw_in(&file_);
  google::protobuf::io::CodedInputStream coded_in(&raw_in);

  coded_in.SetTotalBytesLimit(file_size, file_size);

  uint32_t message_count;
  coded_in.ReadLittleEndian32(&message_count);

  if (message_count == 0) {
    LOG(ERROR) << "No messages in file.";
    return false;
  }

  Transaction transaction(LogicalTime::sample());

  std::unordered_map<Id, Chunk*> existing_chunks;

  for (size_t i = 0; i < message_count; ++i) {
    uint32_t msg_size;
    if (!coded_in.ReadVarint32(&msg_size)) {
      LOG(ERROR) << "Could not read message size."
                 << " while reading message " << i + 1 << " of "
                 << message_count << ".";
      return false;
    }
    if (msg_size == 0) {
      LOG(ERROR) << "Could not read message: size=0."
                 << " while reading message " << i + 1 << " of "
                 << message_count << ".";
      return false;
    }

    std::string input_string;
    if (!coded_in.ReadString(&input_string, msg_size)) {
      LOG(ERROR) << "Could not read message data"
                 << " while reading message " << i + 1 << " of "
                 << message_count << ".";
      return false;
    }

    std::shared_ptr<Revision> revision(new Revision);
    revision->ParseFromString(input_string);

    Id chunk_id;
    CHECK(revision->get(NetTable::kChunkIdField, &chunk_id));
    Chunk* chunk = nullptr;
    std::unordered_map<Id, Chunk*>::iterator it =
        existing_chunks.find(chunk_id);
    if (it == existing_chunks.end()) {
      chunk = table_->newChunk(chunk_id);
      existing_chunks.insert(std::make_pair(chunk_id, chunk));
    } else {
      chunk = it->second;
    }
    CHECK_NOTNULL(chunk);

    transaction.insert(table_, chunk, revision);
  }
  bool ok = transaction.commit();
  LOG_IF(WARNING, !ok) << "Transaction commit failed to load data";
  return ok;
}

}  // namespace map_api
