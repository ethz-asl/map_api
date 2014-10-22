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
Type IPC::pop() {
  return popFor<Type>(kEveryone);
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
std::string IPC::popFor(int receiver);
template <>
Id IPC::popFor(int receiver);
template <>
LogicalTime IPC::popFor(int receiver);
template <>
PeerId IPC::popFor(int receiver);

template <typename Type>
void IPC::pushFor(const Type& message, int receiver) {
  pushFor(std::to_string(message), receiver);
}

template <typename Type>
Type IPC::popFor(int receiver) {
  Type return_value;
  std::string string = popFor<std::string>(receiver);
  std::istringstream ss(string);
  ss >> return_value;
  return return_value;
}

}  // namespace map_api

#endif  // MAP_API_IPC_INL_H_
