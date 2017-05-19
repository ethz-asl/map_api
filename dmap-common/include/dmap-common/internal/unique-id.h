#ifndef DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <atomic>
#include <string>

#include "./id.pb.h"

namespace dmap {
class Hub;
}  // namespace dmap
namespace dmap_common {
namespace internal {
class UniqueIdHashSeed {
 public:
  class Key {
    friend dmap::Hub;
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
}  // namespace dmap_common

#endif  // DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  NOLINT
