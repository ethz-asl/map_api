#include "map-api/internal/network-data-log.h"

#include <chrono>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <multiagent-mapping-common/path-utility.h>

namespace map_api {
namespace internal {

NetworkDataLog::NetworkDataLog(const std::string& prefix)
    : logger_(prefix + "_" + common::GenerateDateStringFromCurrentTime() + "_" +
              std::to_string(getpid())) {}

void NetworkDataLog::log(const size_t bytes, const std::string& type) {
  typedef std::chrono::system_clock Clock;
  logger_ << std::chrono::duration_cast<std::chrono::milliseconds>(
                 Clock::now().time_since_epoch()).count() /
                 1000. << " " << bytes << " " << type << std::endl;
}

void NetworkDataLog::getCumSums(const std::string& file_name,
                                NetworkDataLog::TypeCumSums* cum_sums) {
  CHECK_NOTNULL(cum_sums)->clear();
  typedef std::unordered_map<std::string, std::vector<Line>> TypeLines;
  TypeLines lines;
  std::ifstream stream(file_name);
  CHECK(stream.is_open());
  while (true) {
    Line line;
    stream >> line.time >> line.bytes >> line.type;
    if (stream.fail()) {
      break;
    }
    lines[line.type].emplace_back(line);
  }

  for (const TypeLines::value_type& type : lines) {
    (*cum_sums)[type.first].resize(2, type.second.size());
    for (size_t i = 0u; i < type.second.size(); ++i) {
      (*cum_sums)[type.first].col(i) << type.second[i].time,
          type.second[i].bytes;
      if (i > 0u) {
        (*cum_sums)[type.first](1, i) += (*cum_sums)[type.first](1, i - 1);
      }
    }
  }
}

}  // namespace internal
}  // namespace map_api
