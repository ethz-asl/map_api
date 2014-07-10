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
  static void staticHandleFindSuccessorAndFixFinger(
      const Message& request, Message* response);
  static void staticHandleLeave(const Message& request, Message* response);
  static void staticHandleNotifySuccessor(
      const Message& request, Message* response);
  static void staticHandleNotifyPredecessor(
      const Message& request, Message* response);
  /**
   * RPC types
   */
  static const char kPeerResponse[];
  static const char kFindSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kFindSuccessorAndFixFingerRequest[];
  static const char kFindSuccessorAndFixFingerResponse[];
  static const char kLeaveRequest[];
  static const char kNotifySuccessorRequest[];
  static const char kNotifyPredecessorRequest[];

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

  virtual PeerId findSuccessorRpc(
      const PeerId& to, const Key& argument) final override;
  virtual PeerId getPredecessorRpc(const PeerId& to) final override;
  virtual PeerId findSuccessorAndFixFingerRpc(
      const PeerId& to, const Key& query, const Key& finger_base,
      PeerId* actual_finger_node) final override;
  virtual bool leaveRpc(
      const PeerId& to, const PeerId& leaver, const PeerId&leaver_predecessor,
      const PeerId& leaver_successor) final override;
  virtual bool notifySuccessorRpc(
      const PeerId& successor, const PeerId& self) final override;
  virtual bool notifyPredecessorRpc(
      const PeerId& predecessor, const PeerId& self) final override;

  PeerHandler peers_;
};

const char TestChordIndex::kPeerResponse[] =
    "test_chord_index_peer_response";
const char TestChordIndex::kFindSuccessorRequest[] =
    "test_chord_index_find_successor_request";
const char TestChordIndex::kGetPredecessorRequest[] =
    "test_chord_index_get_predecessor_request";
const char TestChordIndex::kFindSuccessorAndFixFingerRequest[] =
    "test_chord_index_find_predecessor_fix_fingers_request";
const char TestChordIndex::kFindSuccessorAndFixFingerResponse[] =
    "test_chord_index_find_predecessor_fix_fingers_response";
const char TestChordIndex::kLeaveRequest[] =
    "test_chord_index_leave_request";
const char TestChordIndex::kNotifySuccessorRequest[] =
    "test_chord_index_notify_successor_request";
const char TestChordIndex::kNotifyPredecessorRequest[] =
    "test_chord_index_notify_predecessor_request";

MAP_API_STRING_MESSAGE(TestChordIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kFindSuccessorRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kFindSuccessorAndFixFingerRequest,
                      proto::TestChordIndexFindSuccessorFixFingerRequest);
MAP_API_PROTO_MESSAGE(TestChordIndex::kFindSuccessorAndFixFingerResponse,
                      proto::TestChordIndexFindSuccessorFixFingerResponse);
MAP_API_PROTO_MESSAGE(TestChordIndex::kLeaveRequest,
                      proto::TestChordIndexLeaveRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kNotifySuccessorRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kNotifyPredecessorRequest);

void TestChordIndex::staticInit() {
  MapApiHub::instance().registerHandler(
      kFindSuccessorRequest, staticHandleFindSuccessor);
  MapApiHub::instance().registerHandler(
      kGetPredecessorRequest, staticHandleGetPredecessor);
  MapApiHub::instance().registerHandler(
      kFindSuccessorAndFixFingerRequest, staticHandleFindSuccessorAndFixFinger);
  MapApiHub::instance().registerHandler(
      kLeaveRequest, staticHandleLeave);
  MapApiHub::instance().registerHandler(
      kNotifySuccessorRequest, staticHandleNotifySuccessor);
  MapApiHub::instance().registerHandler(
      kNotifyPredecessorRequest, staticHandleNotifyPredecessor);
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
  peer_ss << instance().handleFindSuccessor(key);
  response->impose<kPeerResponse>(peer_ss.str());
}

void TestChordIndex::staticHandleGetPredecessor(
    const Message& request, Message* response) {
  CHECK(request.isType<kGetPredecessorRequest>());
  CHECK_NOTNULL(response);
  response->impose<kPeerResponse>(instance().handleGetPredecessor().ipPort());
}

void TestChordIndex::staticHandleFindSuccessorAndFixFinger(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  proto::TestChordIndexFindSuccessorFixFingerRequest fsff_request;
  request.extract<kFindSuccessorAndFixFingerRequest>(&fsff_request);
  PeerId actual_finger, result = instance().handleFindSuccessorAndFixFinger(
      fsff_request.query(), fsff_request.finger_base(), &actual_finger);
  proto::TestChordIndexFindSuccessorFixFingerResponse fsff_response;
  fsff_response.set_result(result.ipPort());
  fsff_response.set_actual_finger(actual_finger.ipPort());
  response->impose<kFindSuccessorAndFixFingerResponse>(fsff_response);
}

void TestChordIndex::staticHandleLeave(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  proto::TestChordIndexLeaveRequest leave_request;
  request.extract<kLeaveRequest>(&leave_request);
  PeerId leaver(leave_request.leaver()),
      predecessor(leave_request.predecessor()),
      successor(leave_request.successor());
  if (instance().handleLeave(leaver, predecessor, successor)) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleNotifySuccessor(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  if (instance().handleNotifySuccessor(PeerId(request.serialized()))) {
    response->ack();
  } else {
    response->decline();
  }
}

void TestChordIndex::staticHandleNotifyPredecessor(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  if (instance().handleNotifyPredecessor(PeerId(request.serialized()))) {
    response->ack();
  } else {
    response->decline();
  }
}

// ========
// REQUESTS
// ========
PeerId TestChordIndex::findSuccessorRpc(
    const PeerId& to, const Key& argument) {
  Message request, response;
  std::ostringstream key_ss;
  key_ss << argument;
  request.impose<kFindSuccessorRequest>(key_ss.str());
  instance().peers_.request(to, &request, &response);
  return PeerId(response.serialized());
}

PeerId TestChordIndex::getPredecessorRpc(const PeerId& to) {
  Message request, response;
  request.impose<kGetPredecessorRequest>();
  instance().peers_.request(to, &request, &response);
  return PeerId(response.serialized());
}

PeerId TestChordIndex::findSuccessorAndFixFingerRpc(
    const PeerId& to, const Key& query, const Key& finger_base,
    PeerId* actual_finger_node) {
  Message request, response;
  proto::TestChordIndexFindSuccessorFixFingerRequest fsff_request;
  fsff_request.set_query(query);
  fsff_request.set_finger_base(finger_base);
  request.impose<kFindSuccessorAndFixFingerRequest>(fsff_request);
  instance().peers_.request(to, &request, &response);
  proto::TestChordIndexFindSuccessorFixFingerResponse fsff_response;
  response.extract<kFindSuccessorAndFixFingerResponse>(&fsff_response);
  *actual_finger_node = PeerId(fsff_response.actual_finger());
  return PeerId(fsff_response.result());
}

bool TestChordIndex::leaveRpc(
    const PeerId& to, const PeerId& leaver, const PeerId&leaver_predecessor,
    const PeerId& leaver_successor) {
  Message request, response;
  proto::TestChordIndexLeaveRequest leave_request;
  leave_request.set_leaver(leaver.ipPort());
  leave_request.set_predecessor(leaver_predecessor.ipPort());
  leave_request.set_successor(leaver_successor.ipPort());
  request.impose<kLeaveRequest>(leave_request);
  instance().peers_.request(to, &request, &response);
  return response.isType<Message::kAck>();
}

bool TestChordIndex::notifySuccessorRpc(
    const PeerId& successor, const PeerId& self) {
  Message request, response;
  request.impose<kNotifySuccessorRequest>(self.ipPort());
  instance().peers_.request(successor, &request, &response);
  return response.isType<Message::kAck>();
}

bool TestChordIndex::notifyPredecessorRpc(
    const PeerId& predecessor, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyPredecessorRequest>(self.ipPort());
  instance().peers_.request(predecessor, &request, &response);
  return response.isType<Message::kAck>();
}

} // namespace map_api
