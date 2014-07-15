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
  static void staticHandleFindSuccessor(
      const Message& request, Message* response);
  static void staticHandleGetPredecessor(
      const Message& request, Message* response);
  static void staticHandleJoin(
      const Message& request, Message* response);
  static void staticHandleNotify(
      const Message& request, Message* response);
  /**
   * RPC types
   */
  static const char kPeerResponse[];
  static const char kFindSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kJoinRequest[];
  static const char kJoinResponse[];
  static const char kJoinRedirect[];
  static const char kNotifyRequest[];

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

  virtual bool findSuccessorRpc(
      const PeerId& to, const Key& argument, PeerId* successor) final override;
  virtual bool getPredecessorRpc(const PeerId& to, PeerId* predecessor)
  final override;
  virtual bool joinRpc(
      const PeerId& to, bool* success, std::vector<PeerId>* fingers,
      PeerId* predecessor, PeerId* redirect) final override;
  virtual bool notifyRpc(
      const PeerId& to, const PeerId& subject) final override;

  PeerHandler peers_;
};

const char TestChordIndex::kPeerResponse[] =
    "test_chord_index_peer_response";
const char TestChordIndex::kFindSuccessorRequest[] =
    "test_chord_index_find_successor_request";
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

MAP_API_STRING_MESSAGE(TestChordIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kFindSuccessorRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kJoinRedirect);
MAP_API_PROTO_MESSAGE(TestChordIndex::kJoinResponse, proto::JoinResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kNotifyRequest);

void TestChordIndex::staticInit() {
  MapApiHub::instance().registerHandler(
      kFindSuccessorRequest, staticHandleFindSuccessor);
  MapApiHub::instance().registerHandler(
      kGetPredecessorRequest, staticHandleGetPredecessor);
  MapApiHub::instance().registerHandler(
      kJoinRequest, staticHandleJoin);
  MapApiHub::instance().registerHandler(
      kNotifyRequest, staticHandleNotify);
}

// ========
// HANDLERS
// ========

void TestChordIndex::staticHandleFindSuccessor(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  Key key;
  std::istringstream key_ss(request.serialized());
  key_ss >> key;
  std::ostringstream peer_ss;
  peer_ss << instance().handleFindSuccessor(key).ipPort();
  response->impose<kPeerResponse>(peer_ss.str());
}

void TestChordIndex::staticHandleGetPredecessor(
    const Message& request, Message* response) {
  CHECK(request.isType<kGetPredecessorRequest>());
  CHECK_NOTNULL(response);
  response->impose<kPeerResponse>(instance().handleGetPredecessor().ipPort());
}

void TestChordIndex::staticHandleJoin(
    const Message& request, Message* response) {
  CHECK(request.isType<kJoinRequest>());
  CHECK_NOTNULL(response);
  PeerId predecessor, redirect;
  std::vector<PeerId> fingers;
  if (instance().handleJoin(PeerId(request.sender()), &fingers,
                            &predecessor, &redirect)) {
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

// ========
// REQUESTS
// ========
bool TestChordIndex::findSuccessorRpc(
    const PeerId& to, const Key& argument, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  std::ostringstream key_ss;
  key_ss << argument;
  request.impose<kFindSuccessorRequest>(key_ss.str());
  if (!instance().peers_.try_request(to, &request, &response)) {
    return false;
  }
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
    const PeerId& successor, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyRequest>(self.ipPort());
  if (!instance().peers_.try_request(successor, &request, &response)) {
    return false;
  }
  return response.isType<Message::kAck>();
}

} // namespace map_api
