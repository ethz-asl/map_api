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

#ifndef MAP_API_COMMON_CONVERSIONS_H_
#define MAP_API_COMMON_CONVERSIONS_H_

#define _USE_MATH_DEFINES
#include <cmath>

// Please use the namespaces below when adding new constants.

constexpr double kMilliSecondsToSeconds = 1e-3;
constexpr double kSecondsToMilliSeconds = 1e3;
constexpr int64_t kMicrosecondsToNanoseconds = 1000;

constexpr double kMillisecondsToMicroseconds = 1e3;

constexpr double kNanosecondsToSeconds = 1e-9;
constexpr double kSecondsToNanoSeconds = 1e9;

constexpr double kRadToDeg = 180.0 / M_PI;
constexpr double kDegToRad = M_PI / 180.0;

namespace common {
namespace conversions {

constexpr size_t kBitsPerByte = 8u;

constexpr size_t kMilliSecondsToNanoSeconds = 1e6;

}  // namespace conversions
}  // namespace common

#endif  // MAP_API_COMMON_CONVERSIONS_H_
