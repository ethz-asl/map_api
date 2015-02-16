#ifndef MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <string>

#include "./id.pb.h"

namespace map_api {
class Hub;
namespace internal {
class uniqueIdHashSeed {
 public:
  class Key {
    friend Hub;
    Key() { }
  };

  uniqueIdHashSeed() : seed_(31u) { }
  static uniqueIdHashSeed& instance() {
    static uniqueIdHashSeed instance;
    return instance;
  }

  void setSeed(const Key&, size_t seed) {
    seed_ = seed;
  }

  size_t seed() const {
    return seed_;
  }

 private:
  std::atomic<size_t> seed_;
};

void generateUnique128BitHash(uint64_t hash[2]);
}  // namespace internal
}  // namespace map_api

#endif  // MAP_API_INTERNAL_UNIQUE_ID_H_  NOLINT
