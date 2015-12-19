#ifndef INTERNAL_NETWORK_DATA_LOG_H_
#define INTERNAL_NETWORK_DATA_LOG_H_

#include <string>
#include <unordered_map>
#include <vector>

#include <multiagent-mapping-common/plain-file-logger.h>

namespace map_api {
namespace internal {

class NetworkDataLog {
 public:
  explicit NetworkDataLog(const std::string& prefix);

  void log(const size_t bytes, const std::string& type);

  typedef std::unordered_map<std::string, Eigen::Matrix2Xd> TypeCumSums;
  static void getCumSums(const std::string& file_name, TypeCumSums* cum_sums);

 private:
  struct Line {
    double time, bytes;
    std::string type;
  };

  common::PlainFileLogger logger_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_NETWORK_DATA_LOG_H_
