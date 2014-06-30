#ifndef MAP_API_NET_TABLE_MANAGER_INL_H_
#define MAP_API_NET_TABLE_MANAGER_INL_H_

namespace map_api {

template<const char* request_type>
bool NetTableManager::routeChunkMetadataRequestOperations(
    const Message& request, Message* response,
    TableMap::iterator* found, Id* chunk_id, PeerId* peer) {
  CHECK_NOTNULL(chunk_id);
  CHECK_NOTNULL(peer);
  proto::ChunkRequestMetadata metadata;
  request.extract<request_type>(&metadata);
  chunk_id->fromHexString(metadata.chunk_id());
  *peer = PeerId(request.sender());
  return routeChunkRequestOperations(metadata, response, found);
}

template<typename RequestType>
bool NetTableManager::routeChunkRequestOperations(
    const RequestType& request, Message* response,
    TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  const std::string& table = request.metadata().table();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(request.metadata().chunk_id()));
  if (!findTable(table, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

} // namespace map_api

#endif /* MAP_API_NET_TABLE_MANAGER_INL_H_ */
