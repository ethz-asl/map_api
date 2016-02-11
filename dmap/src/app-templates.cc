#include "dmap/app-templates.h"

#include <memory>
#include <string>

namespace dmap {

template <>
void objectFromRevision<std::string>(const dmap::Revision& revision,
                                     std::string* result) {
  CHECK_NOTNULL(result);
  CHECK_EQ(revision.customFieldCount(), 1);
  constexpr int kUniqueFieldIndex = 0;
  revision.get(kUniqueFieldIndex, result);
}

template <>
void objectToRevision(const std::string& object, dmap::Revision* revision) {
  CHECK_EQ(revision->customFieldCount(), 1);
  constexpr int kUniqueFieldIndex = 0;
  revision->set(kUniqueFieldIndex, object);
}

}  // namespace dmap
