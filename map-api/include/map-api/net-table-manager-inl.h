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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_NET_TABLE_MANAGER_INL_H_
#define MAP_API_NET_TABLE_MANAGER_INL_H_

#include <string>

#include "map-api/message.h"

namespace map_api {

template <const char* RequestType>
bool NetTableManager::getTableForMetadataRequestOrDecline(
    const Message& request, Message* response, TableMap::iterator* found,
    map_api_common::Id* chunk_id, PeerId* peer) {
  CHECK_NOTNULL(chunk_id);
  CHECK_NOTNULL(peer);
  proto::ChunkRequestMetadata metadata;
  request.extract<RequestType>(&metadata);
  chunk_id->deserialize(metadata.chunk_id());
  *peer = PeerId(request.sender());
  return getTableForRequestWithMetadataOrDecline(metadata, response, found);
}

template <const char* RequestType>
bool NetTableManager::getTableForStringRequestOrDecline(
    const Message& request, Message* response, TableMap::iterator* found,
    PeerId* peer) {
  CHECK_NOTNULL(peer);
  std::string table_name;
  request.extract<RequestType>(&table_name);
  *peer = PeerId(request.sender());
  return getTableForRequestWithStringOrDecline(table_name, response, found);
}

template <typename RequestType>
bool NetTableManager::getTableForRequestWithMetadataOrDecline(
    const RequestType& request, Message* response, TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  const std::string& table = request.metadata().table();
  if (!findTable(table, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

template <typename StringRequestType>
bool NetTableManager::getTableForRequestWithStringOrDecline(
    const StringRequestType& request, Message* response,
    TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  const std::string& table = request.table_name();
  if (!findTable(table, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_MANAGER_INL_H_
