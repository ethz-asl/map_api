#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map-api/logical-time.h>
#include <map-api/message.h>
#include <map-api/peer.h>
#include <map-api/peer-id.h>

DEFINE_int32(request_timeout, 10000,
             "Amount of milliseconds after which a "
             "non-responsive peer is considered disconnected");
DEFINE_int32(socket_linger_ms, 0,
             "Amount of milliseconds for which a socket "
             "waits for outgoing messages to process before closing.");
DEFINE_int32(simulated_lag_ms, 0,
             "Duration in milliseconds of the simulated lag.");
DEFINE_int32(simulated_bandwidth_kbps, 0,
             "Simulated bandwidth in kB/s. 0 means infinite.");

namespace map_api {

Peer::Peer(const PeerId& address, zmq::context_t& context, int socket_type)
    : address_(address), socket_(context, socket_type) {
  std::lock_guard<std::mutex> lock(socket_mutex_);
  try {
    const int linger_ms = FLAGS_socket_linger_ms;
    socket_.setsockopt(ZMQ_LINGER, &linger_ms, sizeof(linger_ms));
    socket_.connect(("tcp://" + address.ipPort()).c_str());
  }
  catch (const std::exception& e) {  // NOLINT
    LOG(FATAL) << "Connection to " << address << " failed";
  }
}

const PeerId& Peer::address() const { return address_; }

void Peer::request(Message* request, Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  CHECK(try_request(request, response)) << "Message " <<
      request->DebugString() << " timed out!";
}

bool Peer::try_request(Message* request, Message* response) {
  return try_request_for(FLAGS_request_timeout, request, response);
}

bool Peer::try_request_for(int timeout_ms, Message* request,
                           Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  request->set_sender(PeerId::self().ipPort());
  request->set_logical_time(LogicalTime::sample().serialize());
  int size = request->ByteSize();
  VLOG(3) << "Message size is " << size;
  void* buffer = malloc(size);
  CHECK(request->SerializeToArray(buffer, size));
  try {
    zmq::message_t message(buffer, size, NULL, NULL);
    {
      std::lock_guard<std::mutex> lock(socket_mutex_);
      usleep(1e3 * FLAGS_simulated_lag_ms);
      simulateBandwidth(message.size());
      socket_.setsockopt(ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms));
      CHECK(socket_.send(message));
      if (!socket_.recv(&message)) {
        LOG(WARNING) << "Try-request of type " << request->type() <<
            " failed for peer " << address_;
        return false;
      }
    }
    // catches silly bugs where a handler forgets to modify the response
    // message, which could be a quite common bug
    CHECK_GT(message.size(), 0u) << "Request was " << request->DebugString();
    CHECK(response->ParseFromArray(message.data(), message.size()));
    LogicalTime::synchronize(LogicalTime(response->logical_time()));
  } catch(const zmq::error_t& e) {
    LOG(FATAL) << e.what() << ", request was " << request->DebugString() <<
        ", sent to " << address_;
  }
  return true;
}

void Peer::simulateBandwidth(size_t byte_size) {
  if (FLAGS_simulated_bandwidth_kbps == 0) {
    return;
  }
  usleep(1000 * byte_size / FLAGS_simulated_bandwidth_kbps);
}

}  // namespace map_api
