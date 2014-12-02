#ifndef MAP_API_PROTO_TABLE_FILE_IO_H_
#define MAP_API_PROTO_TABLE_FILE_IO_H_

#include <fstream>  // NOLINT
#include <string>
#include <utility>
#include <unordered_set>
#include <vector>

#include <google/protobuf/io/gzip_stream.h>

#include "map-api/cr-table.h"
#include "map-api/unique-id.h"
#include "map-api/logical-time.h"

namespace map_api {
class Chunk;
class NetTable;
class Transaction;

typedef std::pair<Id, map_api::LogicalTime> RevisionStamp;
}  // namespace map_api

namespace std {
template <>
struct hash<map_api::RevisionStamp> {
  std::hash<map_api::Id> id_hasher;
  std::hash<map_api::LogicalTime> time_hasher;
  std::size_t operator()(const map_api::RevisionStamp& stamp) const {
    return id_hasher(stamp.first) ^ time_hasher(stamp.second);
  }
};
}  // namespace std

// Stores all revisions from a table to a file.
namespace map_api {
class ProtoTableFileIO {
  std::ios_base::openmode kDefaultOpenmode =
      std::fstream::binary | std::ios_base::in | std::ios_base::out;

 public:
  ProtoTableFileIO(const std::string& filename, map_api::NetTable* table);
  ~ProtoTableFileIO();
  bool storeTableContents(const map_api::LogicalTime& time);
  bool storeTableContents(const map_api::CRTable::RevisionMap& revisions,
                          const std::vector<map_api::Id>& ids_to_store);
  bool restoreTableContents();
  bool restoreTableContents(map_api::Transaction* transaction,
                            std::unordered_map<Id, Chunk*>* existing_chunks);
  void truncFile();

 private:
  static constexpr google::protobuf::io::GzipOutputStream::Format kOutFormat =
      google::protobuf::io::GzipOutputStream::Format::GZIP;
  static constexpr google::protobuf::io::GzipInputStream::Format kInFormat =
      google::protobuf::io::GzipInputStream::Format::GZIP;
  static constexpr int kZipBufferSize = 64;
  static constexpr int kZipCompressionLevel = -1;
  google::protobuf::io::GzipOutputStream::Options zip_options_;

  std::string file_name_;
  map_api::NetTable* table_;
  std::fstream file_;
  std::unordered_set<RevisionStamp> already_stored_items_;
};
}  // namespace map_api
#endif  // MAP_API_PROTO_TABLE_FILE_IO_H_
