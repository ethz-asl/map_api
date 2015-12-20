#include "map-api/internal/chunk-view.h"

#include "map-api/chunk-base.h"

namespace map_api {
namespace internal {

ChunkView::ChunkView(const ChunkBase& chunk, const LogicalTime& view_time)
    : chunk_(chunk), view_time_(view_time) {}

ChunkView::~ChunkView() {}

bool ChunkView::has(const common::Id& id) const {
  return static_cast<bool>(chunk_.constData()->getById(id, view_time_));
}

std::shared_ptr<const Revision> ChunkView::get(const common::Id& id) const {
  return chunk_.constData()->getById(id, view_time_);
}

void ChunkView::getPotentialConflicts(
    const std::unordered_map<common::Id, LogicalTime>& own_continuous_updates,
    std::unordered_map<common::Id, LogicalTime>* result) const {
  CHECK_NOTNULL(result)->clear();
  CHECK(chunk_.isWriteLocked());

  ConstRevisionMap contents;
  // same as "chunk_->dumpItems(LogicalTime::sample(), &contents);" without the
  // locking (because that is already done)
  chunk_.data_container_->dump(LogicalTime::sample(), &contents);

  for (const ConstRevisionMap::value_type& item : contents) {
    LogicalTime update_time = item.second->getUpdateTime();
    // Discard if update before view time.
    if (update_time <= view_time_) {
      continue;
    }

    // Discard if updated by this transaction in a previous commit.
    typedef std::unordered_map<common::Id, LogicalTime> OwnUpdateMap;
    OwnUpdateMap::const_iterator own_update =
        own_continuous_updates.find(item.first);
    if (own_update != own_continuous_updates.end()) {
      if (update_time <= own_update->second) {
        continue;
      }
    }

    CHECK(result->emplace(item.first, update_time).second);
  }
}

}  // namespace internal
}  // namespace map_api
