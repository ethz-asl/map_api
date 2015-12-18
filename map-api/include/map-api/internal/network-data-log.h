#ifndef INTERNAL_NETWORK_DATA_LOG_H_
#define INTERNAL_NETWORK_DATA_LOG_H_

#include <string>
#include <vector>

namespace map_api {

class NetworkDataLog {
 public:
  NetworkDataLog();

  void log(const size_t time, const size_t bytes, const std::string& type);

  static void getCumSum(const std::string& file_name,
                        Eigen::Matrix2Xi* cum_sum);

  static void getCumSumForType(const std::string& file_name,
                               const std::string& type,
                               Eigen::Matrix2Xi* cum_sum);

 private:
  struct Line {
    size_t time, bytes;
    std::string type;
  };

  static void deserialize(const std::string& file_name,
                          std::vector<Line>* result);

  std::string file_name_;
};

}  // namespace map_api

#endif  // INTERNAL_NETWORK_DATA_LOG_H_
