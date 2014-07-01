#include "map-api/logical-time.h"

#include <glog/logging.h>

#include "map-api/peer-id.h"

namespace map_api{

uint64_t LogicalTime::current_ = 1u;
std::mutex LogicalTime::current_mutex_;

LogicalTime::LogicalTime() : value_(0u) {}

LogicalTime::LogicalTime(uint64_t serialized) : value_(serialized) {}

bool LogicalTime::isValid() const {
  return (value_ > 0u);
}

LogicalTime LogicalTime::sample() {
  LogicalTime time;
  std::lock_guard<std::mutex> lock(current_mutex_);
  time.value_ = current_;
  ++current_;
  return time;
}

uint64_t LogicalTime::serialize() const{
  return value_;
}

void LogicalTime::synchronize(const LogicalTime& other_time) {
  std::lock_guard<std::mutex> lock(current_mutex_);
  if (other_time.value_ >= current_) {
    current_ = other_time.value_ + 1u;
  }
  VLOG(3) << "Logical time at " << PeerId::self() << " synced to " <<
      current_;
}

} // namespace map_api
