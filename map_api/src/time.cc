/*
 * time.cc
 *
 *  Created on: Apr 9, 2014
 *      Author: titus
 */

#include <map-api/time.h>

namespace map_api{

Time::Time(int64_t epochMicroseconds) :
    DateTime(Poco::Timestamp(epochMicroseconds)) {}

Time::Time() : DateTime() {}

int64_t Time::serialize() const{
  return timestamp().epochMicroseconds();
}

} // namespace map_api
