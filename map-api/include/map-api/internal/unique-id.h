#ifndef MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define MAP_API_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <string>

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include "./id.pb.h"

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#  pragma GCC diagnostic pop
#endif

namespace map_api {
namespace internal {
std::string generateUniqueHexString();
}  // namespace internal
}  // namespace map_api

#endif  // MAP_API_INTERNAL_UNIQUE_ID_H_  NOLINT
