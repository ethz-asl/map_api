#ifndef TIME_H_
#define TIME_H_

#include <Poco/DateTime.h>

namespace map_api {

/**
 * TODO(tcies) time synchronization
 */
class Time : public Poco::DateTime{
 public:
  /**
   * To deserialize from database.
   */
  Time(int64_t epochMicroseconds);
  /**
   * Current time
   */
  Time();
  int64_t serialize() const;
};

} // namespace map_api

#endif /* TIME_H_ */
