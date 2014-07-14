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
  static void staticHandleNotify(
      const Message& request, Message* response);
  /**
   * RPC types
   */
  static const char kPeerResponse[];
  static const char kFindSuccessorRequest[];
  static const char kGetPredecessorRequest[];
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
  virtual bool notifyRpc(
      const PeerId& to, const PeerId& self) final override;

  PeerHandler peers_;
};

const char TestChordIndex::kPeerResponse[] =
    "test_chord_index_peer_response";
const char TestChordIndex::kFindSuccessorRequest[] =
    "test_chord_index_find_successor_request";
const char TestChordIndex::kGetPredecessorRequest[] =
    "test_chord_index_get_predecessor_request";
const char TestChordIndex::kNotifyRequest[] =
    "test_chord_index_notify_request";

MAP_API_STRING_MESSAGE(TestChordIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(TestChordIndex::kFindSuccessorRequest);
MAP_API_STRING_MESSAGE(TestChordIndex::kNotifyRequest);

void TestChordIndex::staticInit() {
  MapApiHub::instance().registerHandler(
      kFindSuccessorRequest, staticHandleFindSuccessor);
  MapApiHub::instance().registerHandler(
      kGetPredecessorRequest, staticHandleGetPredecessor);
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
