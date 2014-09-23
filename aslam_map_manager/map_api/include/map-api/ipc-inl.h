#ifndef MAP_API_IPC_INL_H_
#define MAP_API_IPC_INL_H_

#include <sstream>  // NOLINT
#include <string>

#include "map-api/logical-time.h"

namespace map_api {

// Need to declare all custom specializations so that the compiler doesn't
// generate them from the below.
template <>
void IPC::push(const std::string& message);
template <>
void IPC::push(const Id& message);
template <>
void IPC::push(const LogicalTime& message);
template <>
void IPC::push(const PeerId& peer_id);
template <>
bool IPC::pop(std::string* destination);
template <>
bool IPC::pop(Id* destination);
template <>
bool IPC::pop(LogicalTime* destination);
template <>
bool IPC::pop(PeerId* destination);

template <typename Type>
void IPC::push(const Type& message) {
  std::ostringstream ss;
  ss << message;
  push(ss.str());
}

template <typename Type>
bool IPC::pop(Type* destination) {
  CHECK_NOTNULL(destination);
  std::string string;
  if (!pop(&string)) {
    return false;
  }
  std::istringstream ss(string);
  ss >> *destination;
  return true;
}

}  // namespace map_api

#endif  // MAP_API_IPC_INL_H_
