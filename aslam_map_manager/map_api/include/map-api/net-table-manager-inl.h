#ifndef MAP_API_NET_TABLE_MANAGER_INL_H_
#define MAP_API_NET_TABLE_MANAGER_INL_H_

namespace map_api {

template<typename RequestType>
bool NetTableManager::routeChunkRequestOperations(
    const RequestType& request, Message* response,
    TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  const std::string& table = request.table();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(request.chunk_id()));
  PeerId from_peer(request.from_peer());
  if (!findTable(table, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

} // namespace map_api

#endif /* MAP_API_NET_TABLE_MANAGER_INL_H_ */
