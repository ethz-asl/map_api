#include "map-api/internal/combined-view.h"

#include <glog/logging.h>

#include "map-api/revision-map.h"

namespace map_api {
namespace internal {

CombinedView::CombinedView(const std::unique_ptr<ViewBase>& complete_view,
                           const OverridingViewBase& override_view)
    : complete_view_(complete_view), override_view_(override_view) {}

CombinedView::~CombinedView() {}

bool CombinedView::has(const common::Id& id) const {
  return override_view_.has(id) ||
         (complete_view_->has(id) && !override_view_.supresses(id));
}

std::shared_ptr<const Revision> CombinedView::get(const common::Id& id) const {
  if (override_view_.has(id)) {
    return override_view_.get(id);
  } else {
    if (complete_view_->has(id)) {
      if (!override_view_.supresses(id)) {
        return complete_view_->get(id);
      }
    }
  }

  return std::shared_ptr<const Revision>();
}

void CombinedView::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();

  ConstRevisionMap override_dump;
  override_view_.dump(&override_dump);

  complete_view_->dump(result);
  for (ConstRevisionMap::iterator it = result->begin(); it != result->end();) {
    if (override_view_.supresses(it->first)) {
      it = result->erase(it);
    } else {
      ++it;
    }
  }

  result->insert(override_dump.begin(), override_dump.end());
}

void CombinedView::getAvailableIds(std::unordered_set<common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  override_view_.getAvailableIds(result);

  std::unordered_set<common::Id> all_unsupressed_ids;
  complete_view_->getAvailableIds(&all_unsupressed_ids);
  for (std::unordered_set<common::Id>::iterator it =
           all_unsupressed_ids.begin();
       it != all_unsupressed_ids.end();) {
    if (override_view_.supresses(*it)) {
      it = all_unsupressed_ids.erase(it);
    } else {
      ++it;
    }
  }
}

void CombinedView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  override_view_.discardKnownUpdates(update_times);
  complete_view_->discardKnownUpdates(update_times);
}

}  // namespace internal
}  // namespace map_api
