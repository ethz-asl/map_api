#ifndef TIME_INL_H_
#define TIME_INL_H_

namespace map_api {

inline bool Time::operator <=(const Time& other) const {
  return value_ <= other.value_;
}

inline bool Time::operator >=(const Time& other) const {
  return value_ >= other.value_;
}

inline bool Time::operator ==(const Time& other) const {
  return value_ == other.value_;
}

} // namespace map_api

#endif /* TIME_INL_H_ */
