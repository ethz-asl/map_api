#include "map-api/internal/unique-id.h"
#include <atomic>
#include <chrono>

namespace map_api {
namespace internal {
void generateUnique128BitHash(uint64_t hash[2]) {
  static_assert(sizeof(size_t) == sizeof(uint64_t),
                "Please adapt the below to your non-64-bit system.");

  static std::atomic<int> counter;
  ++counter;

  hash[0] =
      map_api::internal::uniqueIdHashSeed::instance().seed() ^
      std::hash<int>()(
          std::chrono::high_resolution_clock::now().time_since_epoch().count());
  hash[1] = std::hash<int>()(counter);
}
}  // namespace internal
}  // namespace map_api
