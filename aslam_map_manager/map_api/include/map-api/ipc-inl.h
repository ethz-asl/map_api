#ifndef MAP_API_IPC_INL_H_
#define MAP_API_IPC_INL_H_

#include <sstream>  // NOLINT
#include <string>

#include "map-api/logical-time.h"

namespace map_api {

template <typename Type>
void IPC::push(const Type& message) {
  pushFor(message, kEveryone);
}

template <typename Type>
bool IPC::pop(Type* destination) {
  return popFor(destination, kEveryone);
}

// Need to declare all custom specializations so that the compiler doesn't
// generate them from the below.
template <>
void IPC::pushFor(const std::string& message, int receiver);
template <>
void IPC::pushFor(const Id& message, int receiver);
template <>
void IPC::pushFor(const LogicalTime& message, int receiver);
template <>
void IPC::pushFor(const PeerId& peer_id, int receiver);
template <>
bool IPC::popFor(std::string* destination, int receiver);
template <>
bool IPC::popFor(Id* destination, int receiver);
template <>
bool IPC::popFor(LogicalTime* destination, int receiver);
template <>
bool IPC::popFor(PeerId* destination, int receiver);

template <typename Type>
void IPC::pushFor(const Type& message, int receiver) {
  pushFor(std::to_string(message), receiver);
}

template <typename Type>
bool IPC::popFor(Type* destination, int receiver) {
  CHECK_NOTNULL(destination);
  std::string string;
  if (!popFor(&string, receiver)) {
    return false;
  }
  std::istringstream ss(string);
  ss >> *destination;
  return true;
}

}  // namespace map_api

#endif  // MAP_API_IPC_INL_H_
