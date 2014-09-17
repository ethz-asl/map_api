#ifndef MAP_API_LOGICAL_TIME_INL_H_
#define MAP_API_LOGICAL_TIME_INL_H_

namespace map_api {

inline bool LogicalTime::operator <(const LogicalTime& other) const {
  return value_ < other.value_;
}

inline bool LogicalTime::operator <=(const LogicalTime& other) const {
  return value_ <= other.value_;
}

inline bool LogicalTime::operator >(const LogicalTime& other) const {
  return value_ > other.value_;
}

inline bool LogicalTime::operator >=(const LogicalTime& other) const {
  return value_ >= other.value_;
}

inline bool LogicalTime::operator ==(const LogicalTime& other) const {
  return value_ == other.value_;
}

inline bool LogicalTime::operator !=(const LogicalTime& other) const {
  return value_ != other.value_;
}

} // namespace map_api

#endif /* MAP_API_LOGICAL_TIME_INL_H_ */
