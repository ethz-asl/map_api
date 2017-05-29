#ifndef INTERNAL_OVERRIDING_VIEW_BASE_H_
#define INTERNAL_OVERRIDING_VIEW_BASE_H_

#include "map-api/internal/view-base.h"

namespace map_api {
namespace internal {

class OverridingViewBase : public ViewBase {
 public:
  virtual ~OverridingViewBase();

  // Return true if the given id should be marked as inexistent even if the
  // overridden view contains it.
  virtual bool suppresses(const map_api_common::Id& id) const = 0;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_OVERRIDING_VIEW_BASE_H_
