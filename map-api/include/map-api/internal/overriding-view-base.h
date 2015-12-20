#ifndef INTERNAL_OVERRIDING_VIEW_BASE_H_
#define INTERNAL_OVERRIDING_VIEW_BASE_H_

#include "map-api/internal/view-base.h"

namespace map_api {
namespace internal {

class OverridingViewBase : public ViewBase {
 public:
  virtual ~OverridingViewBase();

  virtual bool supresses(const common::Id& id) const = 0;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_OVERRIDING_VIEW_BASE_H_
