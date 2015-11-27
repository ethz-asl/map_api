#include "map-api/app-templates.h"

#include <memory>
#include <string>

namespace map_api {

template <>
void objectFromRevision<std::string>(const map_api::Revision& revision,
                                     std::string* result) {
  CHECK_NOTNULL(result);
  CHECK_EQ(revision.customFieldCount(), 1);
  constexpr int kUniqueFieldIndex = 0;
  revision.get(kUniqueFieldIndex, result);
}

template <>
void objectToRevision(const std::string& object, map_api::Revision* revision) {
  CHECK_EQ(revision->customFieldCount(), 1);
  constexpr int kUniqueFieldIndex = 0;
  revision->set(kUniqueFieldIndex, object);
}

}  // namespace map_api
