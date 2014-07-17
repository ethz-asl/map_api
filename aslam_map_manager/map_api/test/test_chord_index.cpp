#include "map-api/chord-index.h"
#include "map-api/message.h"
#include "map-api/peer-handler.h"
#include "chord-index.pb.h"

namespace map_api {

class TestChordIndex final : public ChordIndex {
 public:
  virtual ~TestChordIndex() = default;
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
  static void staticHandleGetClosestPrecedingFinger(
      const Message& request, Message* response);
  static void staticHandleGetSuccessor(
      const Message& request, Message* response);
  static void staticHandleGetPredecessor(
      const Message& request, Message* response);
  static void staticHandleJoin(
      const Message& request, Message* response);
  static void staticHandleNotify(
      const Message& request, Message* response);
  static void staticHandleAddData(
      const Message& request, Message* response);
  static void staticHandleRetrieveData(
      const Message& request, Message* response);
  static void staticHandleFetchResponsibilities(
      const Message& request, Message* response);
  /**
   * RPC types
   */
  static const char kPeerResponse[];
  static const char kGetClosestPrecedingFingerRequest[];
  static const char kGetSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kJoinRequest[];
  static const char kJoinResponse[];
  static const char kJoinRedirect[];
  static const char kNotifyRequest[];
  static const char kAddDataRequest[];
  static const char kRetrieveDataRequest[];
  static const char kRetrieveDataResponse[];
  static const char kFetchResponsibilitiesRequest[];
  static const char kFetchResponsibilitiesResponse[];

  /**
   * Inits handlers, must be called before core::init
   */
  static void staticInit();

 private:
  /**
   * Singleton- required methods
   */
  TestChordIndex() {}
  TestChordIndex(const TestChordIndex&) = delete;
  TestChordIndex& operator =(const TestChordIndex&) = delete;

  virtual bool getClosestPrecedingFingerRpc(
      const PeerId& to, const Key& key, PeerId* closest_preceding) final override;
  virtual bool getSuccessorRpc(const PeerId& to, PeerId* predecessor)
  final override;
  virtual bool getPredecessorRpc(const PeerId& to, PeerId* predecessor)
  final override;
  virtual bool joinRpc(
      const PeerId& to, bool* success, std::vector<PeerId>* fingers,
      PeerId* predecessor, PeerId* redirect) final override;
  virtual bool notifyRpc(
      const PeerId& to, const PeerId& subject) final override;
  virtual bool addDataRpc(
      const PeerId& to, const std::string& key, const std::string& value)
  final override;
  virtual bool retrieveDataRpc(
      const PeerId& to, const std::string& key, std::string* value)
  final override;
  virtual bool fetchResponsibilitiesRpc(
      const PeerId& to, DataMap* responsibilities) final override;

  PeerHandler peers_;
};

const char TestChordIndex::kPeerResponse[] =
    "test_chord_index_peer_response";
const char TestChordIndex::kGetClosestPrecedingFingerRequest[] =
    "test_chord_index_get_closest_preceding_finger_request";
const char TestChordIndex::kGetSuccessorRequest[] =
    "test_chord_index_get_successor_request";
const char TestChordIndex::kGetPredecessorRequest[] =
    "test_chord_index_get_predecessor_request";
const char TestChordIndex::kJoinRequest[] =
    "test_chord_index_join_request";
const char TestChordIndex::kJoinResponse[] =
    "test_chord_index_join_response";
const char TestChordIndex::kJoinRedirect[] =
    "test_chord_index_join_redirect";
const char TestChordIndex::kNotifyRequest[] =
    "test_chord_index_notify_request";
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

MAP_API_STRING_MESSAGE(TestChordIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kGetClosestPrecedingFingerRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kJoinRedirect);
MAP_API_PROTO_MESSAGE(TestChordIndex::kJoinResponse, proto::JoinResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kNotifyRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kAddDataRequest, proto::AddDataRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kRetrieveDataRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kRetrieveDataResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kFetchResponsibilitiesResponse,
                      proto::FetchResponsibilitiesResponse);

void TestChordIndex::staticInit() {
  MapApiHub::instance().registerHandler(
      kGetClosestPrecedingFingerRequest, staticHandleGetClosestPrecedingFinger);
  MapApiHub::instance().registerHandler(
      kGetSuccessorRequest, staticHandleGetSuccessor);
  MapApiHub::instance().registerHandler(
      kGetPredecessorRequest, staticHandleGetPredecessor);
  MapApiHub::instance().registerHandler(
      kJoinRequest, staticHandleJoin);
  MapApiHub::instance().registerHandler(
      kNotifyRequest, staticHandleNotify);
  MapApiHub::instance().registerHandler(
      kAddDataRequest, staticHandleAddData);
  MapApiHub::instance().registerHandler(
      kRetrieveDataRequest, staticHandleRetrieveData);
  MapApiHub::instance().registerHandler(
      kFetchResponsibilitiesRequest, staticHandleFetchResponsibilities);
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

void TestChordIndex::staticHandleGetSuccessor(
    const Message& request, Message* response) {
  CHECK(request.isType<kGetSuccessorRequest>());
  CHECK_NOTNULL(response);
  PeerId successor;
  if (!instance().handleGetSuccessor(&successor)) {
    response->decline();
    return;
  }
  response->impose<kPeerResponse>(successor.ipPort());
}

void TestChordIndex::staticHandleGetPredecessor(
    const Message& request, Message* response) {
  CHECK(request.isType<kGetPredecessorRequest>());
  CHECK_NOTNULL(response);
  PeerId predecessor;
  if (!instance().handleGetPredecessor(&predecessor)) {
    response->decline();
    return;
  }
  response->impose<kPeerResponse>(predecessor.ipPort());
}

void TestChordIndex::staticHandleJoin(
    const Message& request, Message* response) {
  CHECK(request.isType<kJoinRequest>());
  CHECK_NOTNULL(response);
  PeerId predecessor, redirect;
  std::vector<PeerId> fingers;
  bool success;
  if (!instance().handleJoin(PeerId(request.sender()), &success, &fingers,
                             &predecessor, &redirect)) {
    response->decline();
    return;
  }
  if (success) {
    proto::JoinResponse join_response;
    for (const PeerId& finger : fingers) {
      join_response.add_fingers(finger.ipPort());
    }
    join_response.set_predecessor(predecessor.ipPort());
    response->impose<kJoinResponse>(join_response);
  } else {
    response->impose<kJoinRedirect>(redirect.ipPort());
  }
}

void TestChordIndex::staticHandleNotify(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  instance().handleNotify(PeerId(request.serialized()));
  response->ack();
}

void TestChordIndex::staticHandleAddData(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  proto::AddDataRequest add_data_request;
  request.extract<kAddDataRequest>(&add_data_request);
  CHECK(add_data_request.has_key());
  CHECK(add_data_request.has_value());
  if (instance().handleAddData(
      add_data_request.key(), add_data_request.value())) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleRetrieveData(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  std::string key, value;
  request.extract<kRetrieveDataRequest>(&key);
  if (instance().handleRetrieveData(key, &value)) {
    response->impose<kRetrieveDataResponse>(value);
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleFetchResponsibilities(
    const Message& request, Message* response) {
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

// ========
// REQUESTS
// ========
bool TestChordIndex::getClosestPrecedingFingerRpc(
    const PeerId& to, const Key& key, PeerId* result) {
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

bool TestChordIndex::joinRpc(
    const PeerId& to, bool* success, std::vector<PeerId>* fingers,
    PeerId* predecessor, PeerId* redirect) {
  CHECK_NOTNULL(success);
  CHECK_NOTNULL(fingers);
  CHECK_NOTNULL(predecessor);
  CHECK_NOTNULL(redirect);
  Message request, response;
  request.impose<kJoinRequest>();
  if (!instance().peers_.try_request(to, &request, &response)) {
    LOG(WARNING) << "Can't reach " << to;
    return false;
  }
  if (response.isType<Message::kDecline>()) {
    return false;
  }
  if (response.isType<kJoinResponse>()) {
    proto::JoinResponse join_response;
    response.extract<kJoinResponse>(&join_response);
    *success = true;
    fingers->clear();
    for (int i = 0; i < join_response.fingers_size(); ++i) {
      fingers->push_back(PeerId(join_response.fingers(i)));
    }
    *predecessor = PeerId(join_response.predecessor());
  } else {
    CHECK(response.isType<kJoinRedirect>());
    *success = false;
    std::string redirect_string;
    response.extract<kJoinRedirect>(&redirect_string);
    *redirect = PeerId(redirect_string);
  }
  return true;
}

bool TestChordIndex::notifyRpc(
    const PeerId& to, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyRequest>(self.ipPort());
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

bool TestChordIndex::addDataRpc(
    const PeerId& to, const std::string& key, const std::string& value) {
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

bool TestChordIndex::retrieveDataRpc(
    const PeerId& to, const std::string& key, std::string* value) {
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

bool TestChordIndex::fetchResponsibilitiesRpc(
    const PeerId& to, DataMap* responsibilities) {
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

} // namespace map_api
