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
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef DMAP_COMMON_PATH_UTILITY_H_
#define DMAP_COMMON_PATH_UTILITY_H_

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <functional>
#include <queue>
#include <regex>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace map_api_common {

inline std::string GenerateDateString(const time_t& epoch_time) {
  struct tm tm_localtime;
  localtime_r(&epoch_time, &tm_localtime);
  std::string filename = std::string(512, 0);
  int n = strftime(&filename[0], filename.length(),
                   "%Y_%m_%d_%H_%M_%S", &tm_localtime);
  CHECK_GT(n, 0) << "Error constructing datestring";
  filename.resize(n);
  return filename;
}

inline std::string GenerateDateStringFromCurrentTime() {
  time_t epoch_time = time(NULL);
  return GenerateDateString(epoch_time);
}

}  // namespace map_api_common

#endif  // DMAP_COMMON_PATH_UTILITY_H_
