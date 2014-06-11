#ifndef TIME_H_
#define TIME_H_

#include <cstdint>
#include <iostream>

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

namespace std {

inline ostream& operator<<(ostream& out, const map_api::Time& time) {
  out << "Time(" << time.serialize() << ")";
  return out;
}

}

#include "map-api/time-inl.h"

#endif /* TIME_H_ */
