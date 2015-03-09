#ifndef MAP_API_NET_TABLE_MANAGER_INL_H_
#define MAP_API_NET_TABLE_MANAGER_INL_H_

#include <string>

#include "map-api/message.h"

namespace map_api {

template <const char* RequestType>
bool NetTableManager::getTableForMetadataRequestOrDecline(
    const Message& request, Message* response, TableMap::iterator* found,
    common::Id* chunk_id, PeerId* peer) {
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
  return getTableForRequestWithMetadataOrDecline(table_name, response, found);
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

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_MANAGER_INL_H_
