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

namespace dmap_common {

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

}  // namespace dmap_common

#endif  // DMAP_COMMON_PATH_UTILITY_H_
