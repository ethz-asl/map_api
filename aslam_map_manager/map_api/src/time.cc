#include "map-api/time.h"

#include <chrono>

namespace map_api{

Time::Time(int64_t nanoseconds) : value_(nanoseconds) {}

Time::Time() {
  std::chrono::high_resolution_clock::duration current =
      std::chrono::high_resolution_clock::now().time_since_epoch();
  using std::chrono::duration_cast;
  using std::chrono::nanoseconds;
  // count() specified to return at least 64 bits
  value_ = duration_cast<nanoseconds>(current).count();
}

int64_t Time::serialize() const{
  return value_;
}

} // namespace map_api
