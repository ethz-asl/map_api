#ifndef MAP_API_PROTO_TABLE_FILE_IO_H_
#define MAP_API_PROTO_TABLE_FILE_IO_H_
#include <functional>
#include <fstream>  // NOLINT
#include <memory>
#include <string>
#include <utility>
#include <unordered_set>
#include <map-api/net-table.h>

#include <google/protobuf/io/gzip_stream.h>

namespace map_api {
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
 public:
  ProtoTableFileIO(const std::string& filename, map_api::NetTable* table);
  ~ProtoTableFileIO();
  bool storeTableContents(const map_api::LogicalTime& time);
  bool storeTableContents(const map_api::CRTable::RevisionMap& revisions);
  bool restoreTableContents();
  bool restoreTableContents(map_api::Transaction* transaction);

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
