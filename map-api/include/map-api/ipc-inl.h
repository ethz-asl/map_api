// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_IPC_INL_H_
#define MAP_API_IPC_INL_H_

#include <sstream>  // NOLINT
#include <string>

namespace map_api_common {
class Id;
}  // namespace common

namespace map_api {
class LogicalTime;
class PeerId;

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
void IPC::pushFor(const map_api_common::Id& message, int receiver);
template <>
void IPC::pushFor(const LogicalTime& message, int receiver);
template <>
void IPC::pushFor(const PeerId& peer_id, int receiver);
template <>
std::string IPC::popFor(int receiver);
template <>
map_api_common::Id IPC::popFor(int receiver);
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
