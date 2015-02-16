#include "map-api/net-table-index.h"

#include "map-api/message.h"
#include "multiagent-mapping-common/unique-id.h"
#include "./chord-index.pb.h"
#include "./net-table.pb.h"

namespace map_api {

NetTableIndex::NetTableIndex(const std::string& table_name)
: table_name_(table_name) {}

NetTableIndex::~NetTableIndex() {}

void NetTableIndex::announcePosession(const common::Id& chunk_id) {
  std::string peers_string;
  proto::PeerList peers;
  if (!retrieveData(chunk_id.hexString(), &peers_string)) {
    peers.add_peers(PeerId::self().ipPort());
  } else {
    CHECK(peers.ParseFromString(peers_string));
    peers.add_peers(PeerId::self().ipPort());
  }
  CHECK(addData(chunk_id.hexString(), peers.SerializeAsString()));
}

void NetTableIndex::seekPeers(
    const common::Id& chunk_id, std::unordered_set<PeerId>* peers) {
  CHECK_NOTNULL(peers);
  std::string peers_string;
  proto::PeerList peers_proto;
  // because of the simultaneous topology change and retrieve - problem,
  // requests can occasionally fail (catching forever-blocks)
  for (int i = 0; !retrieveData(chunk_id.hexString(), &peers_string); ++i) {
    CHECK_LT(i, 1000) << "Retrieval of chunk" << chunk_id << " from index "\
        "timed out!";
    // corresponds to one second of topology turmoil
    usleep(1000);
  }
  CHECK(peers_proto.ParseFromString(peers_string));
  CHECK_GT(peers_proto.peers_size(), 0);
  for (int i = 0; i < peers_proto.peers_size(); ++i) {
    peers->insert(PeerId(peers_proto.peers(i)));
  }
}

const char NetTableIndex::kRoutedChordRequest[] =
    "map_api_net_table_index_request";
// Because the requests are routed, we don't need to be careful with the choice
// of name!
const char NetTableIndex::kPeerResponse[] = "peer_response";
const char NetTableIndex::kGetClosestPrecedingFingerRequest[] =
    "get_closest_preceding_finger_request";
const char NetTableIndex::kGetSuccessorRequest[] = "get_successor_request";
const char NetTableIndex::kGetPredecessorRequest[] = "get_predecessor_request";
const char NetTableIndex::kLockRequest[] = "lock_request";
const char NetTableIndex::kUnlockRequest[] = "unlock_request";
const char NetTableIndex::kNotifyRequest[] = "notify_request";
const char NetTableIndex::kReplaceRequest[] = "replace_request";
const char NetTableIndex::kAddDataRequest[] = "add_data_request";
const char NetTableIndex::kRetrieveDataRequest[] = "retrieve_data_request";
const char NetTableIndex::kRetrieveDataResponse[] = "retrieve_data_response";
const char NetTableIndex::kFetchResponsibilitiesRequest[] =
    "fetch_responsibilities_request";
const char NetTableIndex::kFetchResponsibilitiesResponse[] =
    "fetch_responsibilities_response";
const char NetTableIndex::kPushResponsibilitiesRequest[] =
    "push_responsibilities_response";

MAP_API_PROTO_MESSAGE(NetTableIndex::kRoutedChordRequest,
                      proto::RoutedChordRequest);

MAP_API_STRING_MESSAGE(NetTableIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(NetTableIndex::kGetClosestPrecedingFingerRequest);
MAP_API_STRING_MESSAGE(NetTableIndex::kNotifyRequest);
MAP_API_PROTO_MESSAGE(NetTableIndex::kReplaceRequest, proto::ReplaceRequest);
MAP_API_PROTO_MESSAGE(NetTableIndex::kAddDataRequest, proto::AddDataRequest);
MAP_API_STRING_MESSAGE(NetTableIndex::kRetrieveDataRequest);
MAP_API_STRING_MESSAGE(NetTableIndex::kRetrieveDataResponse);
MAP_API_PROTO_MESSAGE(NetTableIndex::kFetchResponsibilitiesResponse,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(NetTableIndex::kPushResponsibilitiesRequest,
                      proto::FetchResponsibilitiesResponse);

void NetTableIndex::handleRoutedRequest(
    const Message& routed_request_message, Message* response) {
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
    if (handleAddData(
        add_data_request.key(), add_data_request.value())) {
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

  LOG(FATAL) << "Net table index can't handle request of type " <<
      request.type();
}

// ========
// REQUESTS
// ========
bool NetTableIndex::rpc(
    const PeerId& to, const Message& request, Message* response) {
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

bool NetTableIndex::getClosestPrecedingFingerRpc(
    const PeerId& to, const Key& key, PeerId* result) {
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

bool NetTableIndex::getSuccessorRpc(const PeerId& to, PeerId* result) {
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

bool NetTableIndex::getPredecessorRpc(const PeerId& to, PeerId* result) {
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

bool NetTableIndex::lockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kLockRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool NetTableIndex::unlockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kUnlockRequest>();
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool NetTableIndex::notifyRpc(
    const PeerId& to, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyRequest>(self.ipPort());
  if (!rpc(to, request, &response)) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool NetTableIndex::replaceRpc(
    const PeerId& to, const PeerId& old_peer, const PeerId& new_peer) {
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

bool NetTableIndex::addDataRpc(
    const PeerId& to, const std::string& key, const std::string& value) {
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

bool NetTableIndex::retrieveDataRpc(
    const PeerId& to, const std::string& key, std::string* value) {
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

bool NetTableIndex::fetchResponsibilitiesRpc(
    const PeerId& to, DataMap* responsibilities) {
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

bool NetTableIndex::pushResponsibilitiesRpc(
    const PeerId& to, const DataMap& responsibilities) {
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
