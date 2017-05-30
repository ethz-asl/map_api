// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_PROTO_TABLE_FILE_IO_H_
#define MAP_API_PROTO_TABLE_FILE_IO_H_

#include <fstream>  // NOLINT
#include <string>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <google/protobuf/io/gzip_stream.h>
#include <map-api-common/unique-id.h>

#include "map-api/logical-time.h"

namespace map_api {
class ChunkBase;
class ConstRevisionMap;
class NetTable;
class Transaction;

typedef std::pair<map_api_common::Id, map_api::LogicalTime> RevisionStamp;
}  // namespace map_api

namespace std {
template <>
struct hash<map_api::RevisionStamp> {
  std::hash<map_api_common::Id> id_hasher;
  std::hash<map_api::LogicalTime> time_hasher;
  std::size_t operator()(const map_api::RevisionStamp& stamp) const {
    return id_hasher(stamp.first) ^ time_hasher(stamp.second);
  }
};
}  // namespace std

// Stores all revisions from a table to a file.
namespace map_api {
class ProtoTableFileIO {
  static constexpr std::ios_base::openmode kDefaultOpenmode =
      std::fstream::binary | std::ios_base::in | std::ios_base::out;
  static constexpr std::ios_base::openmode kReadOnlyOpenMode =
      std::fstream::binary | std::ios_base::in;
  static constexpr std::ios_base::openmode kTruncateOpenMode =
      std::fstream::binary | std::ios_base::in | std::ios_base::out |
      std::fstream::trunc;

 public:
  ProtoTableFileIO(const std::string& filename, map_api::NetTable* table);
  ~ProtoTableFileIO();
  bool storeTableContents(const map_api::LogicalTime& time);
  bool storeTableContents(const ConstRevisionMap& revisions,
                          const std::vector<map_api_common::Id>& ids_to_store);
  bool restoreTableContents();
  bool restoreTableContents(
      map_api::Transaction* transaction,
      std::unordered_map<map_api_common::Id, ChunkBase*>* existing_chunks,
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
  map_api::NetTable* table_;
  std::fstream file_;
  std::unordered_set<RevisionStamp> already_stored_items_;
  std::ios_base::openmode open_mode_;
};
}  // namespace map_api
#endif  // MAP_API_PROTO_TABLE_FILE_IO_H_
