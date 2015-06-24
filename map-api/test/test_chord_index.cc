#include "map-api/chord-index.h"
#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/peer-handler.h"
#include "./chord-index.pb.h"

namespace map_api {

class TestChordIndex final : public ChordIndex {
 public:
  virtual ~TestChordIndex() {}
  /**
   * The test chord index is a singleton
   */
  static TestChordIndex& instance() {
    static TestChordIndex object;
    return object;
  }
  /**
   * Static handlers
   */
  static void staticHandleGetClosestPrecedingFinger(const Message& request,
                                                    Message* response);
  static void staticHandleGetSuccessor(const Message& request,
                                       Message* response);
  static void staticHandleGetPredecessor(const Message& request,
                                         Message* response);
  static void staticHandleLock(const Message& request, Message* response);
  static void staticHandleUnlock(const Message& request, Message* response);
  static void staticHandleNotify(const Message& request, Message* response);
  static void staticHandleReplace(const Message& request, Message* response);
  static void staticHandleAddData(const Message& request, Message* response);
  static void staticHandleRetrieveData(const Message& request,
                                       Message* response);
  static void staticHandleFetchResponsibilities(const Message& request,
                                                Message* response);
  static void staticHandlePushResponsibilities(const Message& request,
                                               Message* response);
  static void staticHandleInitReplicator(const Message& request,
                                         Message* response);
  static void staticHandleAppendOnReplicator(const Message& request,
                                             Message* response);

  size_t findPredecessorCountRpcs(const Key& key, PeerId* peer);

  /**
   * RPC types
   */
  static const char kPeerResponse[];
  static const char kGetClosestPrecedingFingerRequest[];
  static const char kGetSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kLockRequest[];
  static const char kUnlockRequest[];
  static const char kNotifyRequest[];
  static const char kReplaceRequest[];
  static const char kAddDataRequest[];
  static const char kRetrieveDataRequest[];
  static const char kRetrieveDataResponse[];
  static const char kFetchResponsibilitiesRequest[];
  static const char kFetchResponsibilitiesResponse[];
  static const char kPushResponsibilitiesRequest[];
  static const char kInitReplicatorRequest[];
  static const char kAppendReplicationDataRequest[];

  /**
   * Inits handlers, must be called before core::init
   */
  static void staticInit();

 private:
  /**
   * Singleton- required methods
   */
  TestChordIndex() = default;
  TestChordIndex(const TestChordIndex&) = delete;
  TestChordIndex& operator=(const TestChordIndex&) = delete;

  virtual bool getClosestPrecedingFingerRpc(const PeerId& to, const Key& key,
                                            PeerId* closest_preceding)
      final override;
  virtual bool getSuccessorRpc(const PeerId& to,
                               PeerId* predecessor) final override;
  virtual bool getPredecessorRpc(const PeerId& to,
                                 PeerId* predecessor) final override;
  virtual ChordIndex::RpcStatus lockRpc(const PeerId& to) final override;
  virtual ChordIndex::RpcStatus unlockRpc(const PeerId& to) final override;
  virtual bool notifyRpc(const PeerId& to, const PeerId& subject,
                         proto::NotifySender sender_type) final override;
  virtual bool replaceRpc(const PeerId& to, const PeerId& old_peer,
                          const PeerId& new_peer) final override;
  virtual bool addDataRpc(const PeerId& to, const std::string& key,
                          const std::string& value) final override;
  virtual bool retrieveDataRpc(const PeerId& to, const std::string& key,
                               std::string* value) final override;
  virtual bool fetchResponsibilitiesRpc(
      const PeerId& to, DataMap* responsibilities) final override;
  virtual bool pushResponsibilitiesRpc(
      const PeerId& to, const DataMap& responsibilities) final override;

  virtual bool initReplicatorRpc(const PeerId& to, size_t index,
                                 const DataMap& data) final override;
  virtual bool appendOnReplicatorRpc(const PeerId& to, size_t index,
                                     const DataMap& data) final override;

  PeerHandler peers_;
};

const char TestChordIndex::kPeerResponse[] = "test_chord_index_peer_response";
const char TestChordIndex::kGetClosestPrecedingFingerRequest[] =
    "test_chord_index_get_closest_preceding_finger_request";
const char TestChordIndex::kGetSuccessorRequest[] =
    "test_chord_index_get_successor_request";
const char TestChordIndex::kGetPredecessorRequest[] =
    "test_chord_index_get_predecessor_request";
const char TestChordIndex::kLockRequest[] = "test_chord_index_lock_request";
const char TestChordIndex::kUnlockRequest[] = "test_chord_index_unlock_request";
const char TestChordIndex::kNotifyRequest[] = "test_chord_index_notify_request";
const char TestChordIndex::kReplaceRequest[] =
    "test_chord_index_replace_request";
const char TestChordIndex::kAddDataRequest[] =
    "test_chord_index_add_data_request";
const char TestChordIndex::kRetrieveDataRequest[] =
    "test_chord_index_retrieve_data_request";
const char TestChordIndex::kRetrieveDataResponse[] =
    "test_chord_index_retrieve_data_response";
const char TestChordIndex::kFetchResponsibilitiesRequest[] =
    "test_chord_index_fetch_responsibilities_request";
const char TestChordIndex::kFetchResponsibilitiesResponse[] =
    "test_chord_index_fetch_responsibilities_response";
const char TestChordIndex::kPushResponsibilitiesRequest[] =
    "test_chord_index_push_responsibilities_response";
const char TestChordIndex::kInitReplicatorRequest[] =
    "test_chord_index_init_chord_replicator";
const char TestChordIndex::kAppendReplicationDataRequest[] =
    "test_chord_index_append_chord_replication_data";

MAP_API_STRING_MESSAGE(TestChordIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kGetClosestPrecedingFingerRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kReplaceRequest, proto::ReplaceRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kAddDataRequest, proto::AddDataRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kRetrieveDataRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kRetrieveDataResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kNotifyRequest, proto::NotifyRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kFetchResponsibilitiesResponse,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kPushResponsibilitiesRequest,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kInitReplicatorRequest,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kAppendReplicationDataRequest,
                      proto::FetchResponsibilitiesResponse);

void TestChordIndex::staticInit() {
  Hub::instance().registerHandler(kGetClosestPrecedingFingerRequest,
                                  staticHandleGetClosestPrecedingFinger);
  Hub::instance().registerHandler(kGetSuccessorRequest,
                                  staticHandleGetSuccessor);
  Hub::instance().registerHandler(kGetPredecessorRequest,
                                  staticHandleGetPredecessor);
  Hub::instance().registerHandler(kLockRequest, staticHandleLock);
  Hub::instance().registerHandler(kUnlockRequest, staticHandleUnlock);
  Hub::instance().registerHandler(kNotifyRequest, staticHandleNotify);
  Hub::instance().registerHandler(kReplaceRequest, staticHandleReplace);
  Hub::instance().registerHandler(kAddDataRequest, staticHandleAddData);
  Hub::instance().registerHandler(kRetrieveDataRequest,
                                  staticHandleRetrieveData);
  Hub::instance().registerHandler(kFetchResponsibilitiesRequest,
                                  staticHandleFetchResponsibilities);
  Hub::instance().registerHandler(kPushResponsibilitiesRequest,
                                  staticHandlePushResponsibilities);
  Hub::instance().registerHandler(kInitReplicatorRequest,
                                  staticHandleInitReplicator);
  Hub::instance().registerHandler(kAppendReplicationDataRequest,
                                  staticHandleAppendOnReplicator);
}

// ========
// HANDLERS
// ========

void TestChordIndex::staticHandleGetClosestPrecedingFinger(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  Key key;
  std::istringstream key_ss(request.serialized());
  key_ss >> key;
  std::ostringstream peer_ss;
  PeerId closest_preceding;
  if (!instance().handleGetClosestPrecedingFinger(key, &closest_preceding)) {
    response->decline();
    return;
  }
  peer_ss << closest_preceding.ipPort();
  response->impose<kPeerResponse>(peer_ss.str());
}

void TestChordIndex::staticHandleGetSuccessor(const Message& request,
                                              Message* response) {
  CHECK(request.isType<kGetSuccessorRequest>());
  CHECK_NOTNULL(response);
  PeerId successor;
  if (!instance().handleGetSuccessor(&successor)) {
    response->decline();
    return;
  }
  response->impose<kPeerResponse>(successor.ipPort());
}

void TestChordIndex::staticHandleGetPredecessor(const Message& request,
                                                Message* response) {
  CHECK(request.isType<kGetPredecessorRequest>());
  CHECK_NOTNULL(response);
  PeerId predecessor;
  if (!instance().handleGetPredecessor(&predecessor)) {
    response->decline();
    return;
  }
  response->impose<kPeerResponse>(predecessor.ipPort());
}

void TestChordIndex::staticHandleLock(const Message& request,
                                      Message* response) {
  CHECK_NOTNULL(response);
  PeerId requester(request.sender());
  if (instance().handleLock(requester)) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleUnlock(const Message& request,
                                        Message* response) {
  CHECK_NOTNULL(response);
  PeerId requester(request.sender());
  if (instance().handleUnlock(requester)) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleNotify(const Message& request,
                                        Message* response) {
  CHECK_NOTNULL(response);
  proto::NotifyRequest notify_request;
  request.extract<kNotifyRequest>(&notify_request);
  if (instance().handleNotify(PeerId(notify_request.peer_id()),
                              notify_request.sender_type())) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleReplace(const Message& request,
                                         Message* response) {
  CHECK_NOTNULL(response);
  proto::ReplaceRequest replace_request;
  request.extract<kReplaceRequest>(&replace_request);
  if (instance().handleReplace(PeerId(replace_request.old_peer()),
                               PeerId(replace_request.new_peer()))) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleAddData(const Message& request,
                                         Message* response) {
  CHECK_NOTNULL(response);
  proto::AddDataRequest add_data_request;
  request.extract<kAddDataRequest>(&add_data_request);
  CHECK(add_data_request.has_key());
  CHECK(add_data_request.has_value());
  if (instance().handleAddData(add_data_request.key(),
                               add_data_request.value())) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleRetrieveData(const Message& request,
                                              Message* response) {
  CHECK_NOTNULL(response);
  std::string key, value;
  request.extract<kRetrieveDataRequest>(&key);
  if (instance().handleRetrieveData(key, &value)) {
    response->impose<kRetrieveDataResponse>(value);
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleFetchResponsibilities(const Message& request,
                                                       Message* response) {
  CHECK_NOTNULL(response);
  DataMap data;
  PeerId requester = PeerId(request.sender());
  CHECK(request.isType<kFetchResponsibilitiesRequest>());
  if (instance().handleFetchResponsibilities(requester, &data)) {
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
}

void TestChordIndex::staticHandlePushResponsibilities(const Message& request,
                                                      Message* response) {
  CHECK_NOTNULL(response);
  DataMap data;
  proto::FetchResponsibilitiesResponse push_request;
  request.extract<kPushResponsibilitiesRequest>(&push_request);
  for (int i = 0; i < push_request.data_size(); ++i) {
    data[push_request.data(i).key()] = push_request.data(i).value();
  }
  if (instance().handlePushResponsibilities(data)) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleInitReplicator(const Message& request,
                                                Message* response) {
  CHECK_NOTNULL(response);
  DataMap data;
  proto::FetchResponsibilitiesResponse init_request;
  request.extract<kInitReplicatorRequest>(&init_request);
  for (int i = 0; i < init_request.data_size(); ++i) {
    data[init_request.data(i).key()] = init_request.data(i).value();
  }
  if (instance().handleInitReplicator(init_request.replicator_index(), data,
                                      request.sender())) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleAppendOnReplicator(const Message& request,
                                                    Message* response) {
  CHECK_NOTNULL(response);
  DataMap data;
  proto::FetchResponsibilitiesResponse replication_request;
  request.extract<kAppendReplicationDataRequest>(&replication_request);
  for (int i = 0; i < replication_request.data_size(); ++i) {
    data[replication_request.data(i).key()] =
        replication_request.data(i).value();
  }
  if (instance().handleAppendOnReplicator(
          replication_request.replicator_index(), data, request.sender())) {
    response->ack();
  } else {
    response->decline();
  }
}

size_t TestChordIndex::findPredecessorCountRpcs(const Key& key, PeerId* peer) {
  CHECK_NOTNULL(peer);
  size_t count = 0;
  PeerId result = closestPrecedingFinger(key), result_successor;
  CHECK(getSuccessorRpc(result, &result_successor));
  while (!isIn(key, hash(result), hash(result_successor))) {
    ++count;
    CHECK(getClosestPrecedingFingerRpc(result, key, &result));
    // Needed because we are not checking apriori if key predecessor is self.
    if (result == PeerId::self()) {
      break;
    }
    CHECK(getSuccessorRpc(result, &result_successor));
  }
  *peer = result;
  return count;
}

// ========
// REQUESTS
// ========
bool TestChordIndex::getClosestPrecedingFingerRpc(const PeerId& to,
                                                  const Key& key,
                                                  PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  std::ostringstream key_ss;
  key_ss << key;
  request.impose<kGetClosestPrecedingFingerRequest>(key_ss.str());
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool TestChordIndex::getSuccessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetSuccessorRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool TestChordIndex::getPredecessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetPredecessorRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

ChordIndex::RpcStatus TestChordIndex::lockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kLockRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    LOG(WARNING) << "Couldn't reach peer to lock";
    return RpcStatus::RPC_FAILED;
  }
  if (response.isType<Message::kDecline>()) {
    return RpcStatus::DECLINED;
  }
  CHECK(response.isType<Message::kAck>());
  return RpcStatus::SUCCESS;
}

ChordIndex::RpcStatus TestChordIndex::unlockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kUnlockRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    LOG(WARNING) << "Couldn't reach peer to unlock";
    return RpcStatus::RPC_FAILED;
  }
  if (response.isType<Message::kDecline>()) {
    return RpcStatus::DECLINED;
  }
  CHECK(response.isType<Message::kAck>());
  return RpcStatus::SUCCESS;
}

bool TestChordIndex::notifyRpc(const PeerId& to, const PeerId& self,
                               proto::NotifySender sender_type) {
  Message request, response;
  proto::NotifyRequest notify_request;
  notify_request.set_peer_id(self.ipPort());
  notify_request.set_sender_type(sender_type);
  request.impose<kNotifyRequest>(notify_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::replaceRpc(const PeerId& to, const PeerId& old_peer,
                                const PeerId& new_peer) {
  Message request, response;
  proto::ReplaceRequest replace_request;
  replace_request.set_old_peer(old_peer.ipPort());
  replace_request.set_new_peer(new_peer.ipPort());
  request.impose<kReplaceRequest>(replace_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::addDataRpc(const PeerId& to, const std::string& key,
                                const std::string& value) {
  Message request, response;
  proto::AddDataRequest add_data_request;
  add_data_request.set_key(key);
  add_data_request.set_value(value);
  request.impose<kAddDataRequest>(add_data_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::retrieveDataRpc(const PeerId& to, const std::string& key,
                                     std::string* value) {
  CHECK_NOTNULL(value);
  Message request, response;
  request.impose<kRetrieveDataRequest>(key);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  CHECK(response.isType<kRetrieveDataResponse>());
  response.extract<kRetrieveDataResponse>(value);
  return true;
}

bool TestChordIndex::fetchResponsibilitiesRpc(const PeerId& to,
                                              DataMap* responsibilities) {
  CHECK_NOTNULL(responsibilities);
  Message request, response;
  request.impose<kFetchResponsibilitiesRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  CHECK(response.isType<kFetchResponsibilitiesResponse>());
  proto::FetchResponsibilitiesResponse fetch_response;
  response.extract<kFetchResponsibilitiesResponse>(&fetch_response);
  for (int i = 0; i < fetch_response.data_size(); ++i) {
    responsibilities->insert(std::make_pair(fetch_response.data(i).key(),
                                            fetch_response.data(i).value()));
  }
  return true;
}

bool TestChordIndex::pushResponsibilitiesRpc(const PeerId& to,
                                             const DataMap& responsibilities) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : responsibilities) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  request.impose<kPushResponsibilitiesRequest>(push_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::initReplicatorRpc(const PeerId& to, size_t index,
                                       const DataMap& data) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : data) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  push_request.set_replicator_index(index);
  request.impose<kInitReplicatorRequest>(push_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::appendOnReplicatorRpc(const PeerId& to, size_t index,
                                           const DataMap& data) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : data) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  push_request.set_replicator_index(index);
  request.impose<kAppendReplicationDataRequest>(push_request);
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

}  // namespace map_api
