#include "map-api/internal/combined-view.h"

namespace map_api {
namespace internal {

CombinedView::CombinedView(const ViewBase& complete_view,
                           const OverridingViewBase& override_view)
    : complete_view_(complete_view), override_view_(override_view) {}

CombinedView::~CombinedView() {}

bool CombinedView::has(const common::Id& id) const {
  return override_view_.has(id) ||
         (complete_view_.has(id) && !override_view_.supresses(id));
}

std::shared_ptr<const Revision> CombinedView::get(const common::Id& id) const {
  if (override_view_.has(id)) {
    return override_view_.get(id);
  } else {
    if (complete_view_.has(id) && !override_view_.supresses(id)) {
      return complete_view_.get(id);
    }
  }

  return std::shared_ptr<const Revision>();
}

}  // namespace internal
}  // namespace map_api
