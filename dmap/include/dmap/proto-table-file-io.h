#ifndef DMAP_PROTO_TABLE_FILE_IO_H_
#define DMAP_PROTO_TABLE_FILE_IO_H_

#include <fstream>  // NOLINT
#include <string>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <google/protobuf/io/gzip_stream.h>
#include <dmap-common/unique-id.h>

#include "dmap/logical-time.h"

namespace dmap {
class ChunkBase;
class ConstRevisionMap;
class NetTable;
class Transaction;

typedef std::pair<dmap_common::Id, dmap::LogicalTime> RevisionStamp;
}  // namespace dmap

namespace std {
template <>
struct hash<dmap::RevisionStamp> {
  std::hash<dmap_common::Id> id_hasher;
  std::hash<dmap::LogicalTime> time_hasher;
  std::size_t operator()(const dmap::RevisionStamp& stamp) const {
    return id_hasher(stamp.first) ^ time_hasher(stamp.second);
  }
};
}  // namespace std

// Stores all revisions from a table to a file.
namespace dmap {
class ProtoTableFileIO {
  static constexpr std::ios_base::openmode kDefaultOpenmode =
      std::fstream::binary | std::ios_base::in | std::ios_base::out;
  static constexpr std::ios_base::openmode kReadOnlyOpenMode =
      std::fstream::binary | std::ios_base::in;
  static constexpr std::ios_base::openmode kTruncateOpenMode =
      std::fstream::binary | std::ios_base::in | std::ios_base::out |
      std::fstream::trunc;

 public:
  ProtoTableFileIO(const std::string& filename, dmap::NetTable* table);
  ~ProtoTableFileIO();
  bool storeTableContents(const dmap::LogicalTime& time);
  bool storeTableContents(const ConstRevisionMap& revisions,
                          const std::vector<dmap_common::Id>& ids_to_store);
  bool restoreTableContents();
  bool restoreTableContents(
      dmap::Transaction* transaction,
      std::unordered_map<dmap_common::Id, ChunkBase*>* existing_chunks,
      std::mutex* existing_chunks_mutex);
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
  dmap::NetTable* table_;
  std::fstream file_;
  std::unordered_set<RevisionStamp> already_stored_items_;
  std::ios_base::openmode open_mode_;
};
}  // namespace dmap
#endif  // DMAP_PROTO_TABLE_FILE_IO_H_
