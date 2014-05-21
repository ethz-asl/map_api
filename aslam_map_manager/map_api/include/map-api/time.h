#ifndef TIME_H_
#define TIME_H_

#include <cstdint>

namespace map_api {

/**
 * TODO(tcies) time synchronization
 */
class Time {
 public:
  /**
   * To deserialize from database.
   */
  Time(int64_t nanoseconds);
  /**
   * Current time
   */
  Time();

  int64_t serialize() const;

  inline bool operator <=(const Time& other) const;

  inline bool operator >=(const Time& other) const;

  inline bool operator ==(const Time& other) const;

 private:
  int64_t value_;
};

} // namespace map_api

#include "map-api/time-inl.h"

#endif /* TIME_H_ */
