#include "map-api/spatial-index.h"
#include "./net-table.pb.h"

namespace map_api {

SpatialIndex::SpatialIndex(const std::string& table_name,
                           const BoundingBox& bounds,
                           const std::vector<size_t>& subdivision)
    : table_name_(table_name), bounds_(bounds), subdivision_(subdivision) {
  CHECK_EQ(bounds.size(), subdivision.size());
  for (size_t count : subdivision) {
    CHECK_GT(count, 0);
  }
  for (const Range& bound : bounds) {
    CHECK_LT(bound.first, bound.second);
  }
}

SpatialIndex::~SpatialIndex() {}

void SpatialIndex::announceChunk(const Id& chunk_id,
                                 const BoundingBox& bounding_box) {
  std::vector<size_t> affected_cell_indices;
  getCellIndices(bounding_box, &affected_cell_indices);

  for (size_t cell_index : affected_cell_indices) {
    std::string chunks_string;
    proto::ChunkList chunks;
    if (retrieveData(typeHack(cell_index), &chunks_string)) {
      CHECK(chunks.ParseFromString(chunks_string));
    }
    chunks.add_chunk_ids(chunk_id.hexString());
    CHECK(addData(typeHack(cell_index), chunks.SerializeAsString()));
  }
}

void SpatialIndex::seekChunks(const BoundingBox& bounding_box,
                              std::unordered_set<Id>* chunk_ids) {
  CHECK_NOTNULL(chunk_ids);
  std::vector<size_t> affected_cell_indices;
  getCellIndices(bounding_box, &affected_cell_indices);

  for (size_t cell_index : affected_cell_indices) {
    std::string chunks_string;
    proto::ChunkList proto_chunk_ids;
    // because of the simultaneous topology change and retrieve - problem,
    // requests can occasionally fail (catching forever-blocks)
    for (int i = 0; !retrieveData(typeHack(cell_index), &chunks_string); ++i) {
      CHECK_LT(i, 1000) << "Retrieval of cell" << cell_index << " from index "
                                                                "timed out!";
      // corresponds to one second of topology turmoil
      usleep(1000);
    }
    CHECK(proto_chunk_ids.ParseFromString(chunks_string));
    CHECK_GT(proto_chunk_ids.chunk_ids_size(), 0);
    for (int i = 0; i < proto_chunk_ids.chunk_ids_size(); ++i) {
      Id chunk_id;
      chunk_id.fromHexString(proto_chunk_ids.chunk_ids(i));
      chunk_ids->insert(chunk_id);
    }
  }
}

const char SpatialIndex::kRoutedChordRequest[] =
    "map_api_spatial_index_request";
// Because the requests are routed, we don't need to be careful with the choice
// of name!
const char SpatialIndex::kPeerResponse[] = "peer_response";
const char SpatialIndex::kGetClosestPrecedingFingerRequest[] =
    "get_closest_preceding_finger_request";
const char SpatialIndex::kGetSuccessorRequest[] = "get_successor_request";
const char SpatialIndex::kGetPredecessorRequest[] = "get_predecessor_request";
const char SpatialIndex::kLockRequest[] = "lock_request";
const char SpatialIndex::kUnlockRequest[] = "unlock_request";
const char SpatialIndex::kNotifyRequest[] = "notify_request";
const char SpatialIndex::kReplaceRequest[] = "replace_request";
const char SpatialIndex::kAddDataRequest[] = "add_data_request";
const char SpatialIndex::kRetrieveDataRequest[] = "retrieve_data_request";
const char SpatialIndex::kRetrieveDataResponse[] = "retrieve_data_response";
const char SpatialIndex::kFetchResponsibilitiesRequest[] =
    "fetch_responsibilities_request";
const char SpatialIndex::kFetchResponsibilitiesResponse[] =
    "fetch_responsibilities_response";
const char SpatialIndex::kPushResponsibilitiesRequest[] =
    "push_responsibilities_response";

MAP_API_PROTO_MESSAGE(SpatialIndex::kRoutedChordRequest,
                      proto::RoutedChordRequest);

MAP_API_STRING_MESSAGE(SpatialIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(SpatialIndex::kGetClosestPrecedingFingerRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kNotifyRequest);
MAP_API_PROTO_MESSAGE(SpatialIndex::kReplaceRequest, proto::ReplaceRequest);
MAP_API_PROTO_MESSAGE(SpatialIndex::kAddDataRequest, proto::AddDataRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kRetrieveDataRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kRetrieveDataResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kFetchResponsibilitiesResponse,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kPushResponsibilitiesRequest,
                      proto::FetchResponsibilitiesResponse);

void SpatialIndex::handleRoutedRequest(const Message& routed_request_message,
                                       Message* response) {
  CHECK_NOTNULL(response);
  proto::RoutedChordRequest routed_request;
  routed_request_message.extract<kRoutedChordRequest>(&routed_request);
  CHECK(routed_request.has_serialized_message());
  Message request;
  CHECK(request.ParseFromString(routed_request.serialized_message()));
  // TODO(tcies) a posteriori, especially given the new routing system,
  // map_api::Message handling in ChordIndex itself could have been a thing
  // the following code is mostly copied from test/test_chord_index.cpp :(

  if (!request.has_sender()) {
    CHECK(routed_request_message.has_sender());
    request.set_sender(routed_request_message.sender());
  }

  if (request.isType<kGetClosestPrecedingFingerRequest>()) {
    Key key;
    std::istringstream key_ss(request.serialized());
    key_ss >> key;
    std::ostringstream peer_ss;
    PeerId closest_preceding;
    if (!handleGetClosestPrecedingFinger(key, &closest_preceding)) {
      response->decline();
      return;
    }
    peer_ss << closest_preceding.ipPort();
    response->impose<kPeerResponse>(peer_ss.str());
    return;
  }

  if (request.isType<kGetSuccessorRequest>()) {
    PeerId successor;
    if (!handleGetSuccessor(&successor)) {
      response->decline();
      return;
    }
    response->impose<kPeerResponse>(successor.ipPort());
    return;
  }

  if (request.isType<kGetPredecessorRequest>()) {
    PeerId predecessor;
    if (!handleGetPredecessor(&predecessor)) {
      response->decline();
      return;
    }
    response->impose<kPeerResponse>(predecessor.ipPort());
    return;
  }

  if (request.isType<kLockRequest>()) {
    PeerId requester(request.sender());
    if (handleLock(requester)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kUnlockRequest>()) {
    PeerId requester(request.sender());
    if (handleUnlock(requester)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kNotifyRequest>()) {
    if (handleNotify(PeerId(request.serialized()))) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kReplaceRequest>()) {
    proto::ReplaceRequest replace_request;
    request.extract<kReplaceRequest>(&replace_request);
    if (handleReplace(PeerId(replace_request.old_peer()),
                      PeerId(replace_request.new_peer()))) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kAddDataRequest>()) {
    proto::AddDataRequest add_data_request;
    request.extract<kAddDataRequest>(&add_data_request);
    CHECK(add_data_request.has_key());
    CHECK(add_data_request.has_value());
    if (handleAddData(add_data_request.key(), add_data_request.value())) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kRetrieveDataRequest>()) {
    std::string key, value;
    request.extract<kRetrieveDataRequest>(&key);
    if (handleRetrieveData(key, &value)) {
      response->impose<kRetrieveDataResponse>(value);
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kFetchResponsibilitiesRequest>()) {
    DataMap data;
    PeerId requester = PeerId(request.sender());
    CHECK(request.isType<kFetchResponsibilitiesRequest>());
    if (handleFetchResponsibilities(requester, &data)) {
      proto::FetchResponsibilitiesResponse fetch_response;
      for (const DataMap::value_type& item : data) {
        proto::AddDataRequest add_request;
        add_request.set_key(item.first);
        add_request.set_value(item.second);
        proto::AddDataRequest* slot = fetch_response.add_data();
        CHECK_NOTNULL(slot);
        *slot = add_request;
      }
      response->impose<kFetchResponsibilitiesResponse>(fetch_response);
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kPushResponsibilitiesRequest>()) {
    DataMap data;
    proto::FetchResponsibilitiesResponse push_request;
    request.extract<kPushResponsibilitiesRequest>(&push_request);
    for (int i = 0; i < push_request.data_size(); ++i) {
      data[push_request.data(i).key()] = push_request.data(i).value();
    }
    if (handlePushResponsibilities(data)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  LOG(FATAL) << "Net table index can't handle request of type "
             << request.type();
}

inline void SpatialIndex::getCellIndices(const BoundingBox& bounding_box,
                                         std::vector<size_t>* indices) const {
  CHECK_NOTNULL(indices);
  indices->clear();
  indices->push_back(0);
  CHECK_EQ(bounds_.size(), bounding_box.size());
  size_t unit = 1;
  for (size_t dimension = 0; dimension < bounds_.size(); ++dimension) {
    CHECK_GE(bounding_box[dimension].first, bounds_[dimension].first);
    CHECK_LT(bounding_box[dimension].first, bounding_box[dimension].second);
    CHECK_LT(bounding_box[dimension].second, bounds_[dimension].second);
    std::vector<size_t> this_dimension_indices;
    std::vector<size_t> product;
    for (size_t i = coefficientOf(dimension, bounding_box[dimension].first);
         i <= coefficientOf(dimension, bounding_box[dimension].second); ++i) {
      this_dimension_indices.push_back(i);
    }
    for (size_t this_dimension_index : this_dimension_indices) {
      for (size_t previous_index : *indices) {
        product.push_back(previous_index + this_dimension_index * unit);
      }
    }
    indices->swap(product);
    unit *= subdivision_[dimension];
  }
}

inline size_t SpatialIndex::coefficientOf(size_t dimension,
                                          double value) const {
  value -= bounds_[dimension].first;
  value *= subdivision_[dimension];
  value /= (bounds_[dimension].second - bounds_[dimension].first);
  return static_cast<size_t>(value);
}

inline std::string SpatialIndex::typeHack(size_t cell_index) {
  std::ostringstream ss;
  ss << cell_index;
  return ss.str();
}

// ========
// REQUESTS
// ========
bool SpatialIndex::rpc(const PeerId& to, const Message& request,
                       Message* response) {
  CHECK_NOTNULL(response);
  Message to_be_sent;
  proto::RoutedChordRequest routed_request;
  routed_request.set_table_name(table_name_);
  routed_request.set_serialized_message(request.SerializeAsString());
  to_be_sent.impose<kRoutedChordRequest>(routed_request);
  if (!peers_.try_request(to, &to_be_sent, response)) {
    return false;
  }
  if (response->isType<Message::kDecline>()) {
    return false;
  }
  return true;
}

bool SpatialIndex::getClosestPrecedingFingerRpc(const PeerId& to,
                                                const Key& key,
                                                PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  std::ostringstream key_ss;
  key_ss << key;
  request.impose<kGetClosestPrecedingFingerRequest>(key_ss.str());
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool SpatialIndex::getSuccessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetSuccessorRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool SpatialIndex::getPredecessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetPredecessorRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool SpatialIndex::lockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kLockRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::unlockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kUnlockRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::notifyRpc(const PeerId& to, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyRequest>(self.ipPort());
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::replaceRpc(const PeerId& to, const PeerId& old_peer,
                              const PeerId& new_peer) {
  Message request, response;
  proto::ReplaceRequest replace_request;
  replace_request.set_old_peer(old_peer.ipPort());
  replace_request.set_new_peer(new_peer.ipPort());
  request.impose<kReplaceRequest>(replace_request);
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::addDataRpc(const PeerId& to, const std::string& key,
                              const std::string& value) {
  Message request, response;
  proto::AddDataRequest add_data_request;
  add_data_request.set_key(key);
  add_data_request.set_value(value);
  request.impose<kAddDataRequest>(add_data_request);
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::retrieveDataRpc(const PeerId& to, const std::string& key,
                                   std::string* value) {
  CHECK_NOTNULL(value);
  Message request, response;
  request.impose<kRetrieveDataRequest>(key);
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<kRetrieveDataResponse>());
  response.extract<kRetrieveDataResponse>(value);
  return true;
}

bool SpatialIndex::fetchResponsibilitiesRpc(const PeerId& to,
                                            DataMap* responsibilities) {
  CHECK_NOTNULL(responsibilities);
  Message request, response;
  request.impose<kFetchResponsibilitiesRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<kFetchResponsibilitiesResponse>());
  proto::FetchResponsibilitiesResponse fetch_response;
  response.extract<kFetchResponsibilitiesResponse>(&fetch_response);
  for (int i = 0; i < fetch_response.data_size(); ++i) {
    responsibilities->emplace(fetch_response.data(i).key(),
                              fetch_response.data(i).value());
  }
  return true;
}

bool SpatialIndex::pushResponsibilitiesRpc(const PeerId& to,
                                           const DataMap& responsibilities) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : responsibilities) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  request.impose<kPushResponsibilitiesRequest>(push_request);
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

} /* namespace map_api */
