#ifndef DMAP_NET_TABLE_MANAGER_INL_H_
#define DMAP_NET_TABLE_MANAGER_INL_H_

#include <string>

#include "dmap/message.h"

namespace dmap {

template <const char* RequestType>
bool NetTableManager::getTableForMetadataRequestOrDecline(
    const Message& request, Message* response, TableMap::iterator* found,
    dmap_common::Id* chunk_id, PeerId* peer) {
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

}  // namespace dmap

#endif  // DMAP_NET_TABLE_MANAGER_INL_H_
