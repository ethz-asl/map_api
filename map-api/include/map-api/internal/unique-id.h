#ifndef MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <string>

#include "./id.pb.h"

namespace map_api {
namespace internal {
void generateUnique128BitHash(uint64_t hash[2]);
}  // namespace internal
}  // namespace map_api

#endif  // MAP_API_INTERNAL_UNIQUE_ID_H_  NOLINT
