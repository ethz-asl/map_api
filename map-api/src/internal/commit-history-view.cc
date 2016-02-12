#include "map-api/internal/commit-history-view.h"

#include "map-api/chunk-base.h"
#include "map-api/logical-time.h"

namespace map_api {
namespace internal {

CommitHistoryView::CommitHistoryView(const History& commit_history,
                                     const ChunkBase& chunk)
    : commit_history_(commit_history), chunk_(chunk) {}

CommitHistoryView::~CommitHistoryView() {}

bool CommitHistoryView::has(const common::Id& id) const {
  return static_cast<bool>(get(id));
}

std::shared_ptr<const Revision> CommitHistoryView::get(const common::Id& id)
    const {
  // Item could be deleted, so we need to attempt to get it.
  History::const_iterator found = commit_history_.find(id);
  if (found != commit_history_.end()) {
    return chunk_.constData()->getById(id, found->second);
  } else {
    return std::shared_ptr<const Revision>();
  }
}

void CommitHistoryView::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();
  for (const History::value_type& history_item : commit_history_) {
    std::shared_ptr<const Revision> item = get(history_item.first);
    if (item) {
      result->emplace(history_item.first, item);
    }
  }
}

void CommitHistoryView::getAvailableIds(std::unordered_set<common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (const History::value_type& history_item : commit_history_) {
    if (has(history_item.first)) {
      result->emplace(history_item.first);
    }
  }
}

void CommitHistoryView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  for (const History::value_type& known_update : commit_history_) {
    UpdateTimes::const_iterator found = update_times->find(known_update.first);
    if (found != update_times->end()) {
      if (found->second <= known_update.second) {
        update_times->erase(found);
      }
    }
  }
}

bool CommitHistoryView::suppresses(const common::Id& id) const {
  History::const_iterator found = commit_history_.find(id);
  if (found != commit_history_.end()) {
    if (!chunk_.constData()->getById(id, found->second)) {
      return true;
    }
  }
  return false;
}

}  // namespace internal
}  // namespace map_api
