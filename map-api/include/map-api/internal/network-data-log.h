#ifndef INTERNAL_NETWORK_DATA_LOG_H_
#define INTERNAL_NETWORK_DATA_LOG_H_

#include <fstream>  // NOLINT
#include <string>
#include <unordered_map>
#include <vector>

#include <Eigen/Dense>

namespace dmap {
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

  std::ofstream logger_;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_NETWORK_DATA_LOG_H_
