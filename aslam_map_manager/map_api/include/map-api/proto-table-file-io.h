#ifndef MAP_API_PROTO_TABLE_FILE_IO_H_
#define MAP_API_PROTO_TABLE_FILE_IO_H_
#include <functional>
#include <fstream>  // NOLINT
#include <memory>
#include <string>
#include <utility>
#include <unordered_set>
#include <map-api/net-table.h>

namespace map_api {
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

namespace map_api {
class ProtoTableFileIO {
 public:
  ProtoTableFileIO(const map_api::NetTable& table, const std::string& filename);
  bool StoreTableContents(const map_api::LogicalTime& time,
                          const map_api::NetTable& table);
  bool ReStoreTableContents(map_api::NetTable* table);

 private:
  std::string table_name_;
  std::string file_name_;
  std::fstream file_;
  std::unordered_set<RevisionStamp> already_stored_items_;
};
}  // namespace map_api
#endif  // MAP_API_PROTO_TABLE_FILE_IO_H_
