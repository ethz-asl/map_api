#ifndef INTERNAL_OVERRIDING_VIEW_BASE_H_
#define INTERNAL_OVERRIDING_VIEW_BASE_H_

#include "dmap/internal/view-base.h"

namespace dmap {
namespace internal {

class OverridingViewBase : public ViewBase {
 public:
  virtual ~OverridingViewBase();

  // Return true if the given id should be marked as inexistent even if the
  // overridden view contains it.
  virtual bool suppresses(const common::Id& id) const = 0;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_OVERRIDING_VIEW_BASE_H_
