#include "dmap-common/internal/unique-id.h"

#include <atomic>
#include <chrono>

namespace dmap_common {
namespace internal {

void generateUnique128BitHash(uint64_t hash[2]) {
  static_assert(sizeof(size_t) == sizeof(uint64_t),
                "Please adapt the below to your non-64-bit system.");

  static std::atomic<int> counter;
  hash[0] =
      UniqueIdHashSeed::instance().seed() ^ std::hash<int>()(
          std::chrono::high_resolution_clock::now().time_since_epoch().count());
  // Increment must happen here, otherwise the sampling is not atomic.
  hash[1] = std::hash<int>()(++counter);
}
}  // namespace internal
}  // namespace dmap_common
