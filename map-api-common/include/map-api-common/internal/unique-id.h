#ifndef DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <atomic>
#include <string>

#include "./id.pb.h"

namespace map_api {
class Hub;
}  // namespace map_api
namespace map_api_common {
namespace internal {
class UniqueIdHashSeed {
 public:
  class Key {
    friend map_api::Hub;
    Key() { }
  };

  UniqueIdHashSeed() : seed_(31u) { }
  static UniqueIdHashSeed& instance() {
    static UniqueIdHashSeed instance;
    return instance;
  }

  void saltSeed(const Key&, size_t salt) {
    seed_ ^= salt;
  }

  size_t seed() const {
    return seed_;
  }

 private:
  std::atomic<size_t> seed_;
};

void generateUnique128BitHash(uint64_t hash[2]);
}  // namespace internal
}  // namespace map_api_common

#endif  // DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  NOLINT
