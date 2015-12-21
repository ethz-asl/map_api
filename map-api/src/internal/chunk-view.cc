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

void ChunkView::dump(ConstRevisionMap* result) const {
  chunk_.constData()->dump(view_time_, result);
}

void ChunkView::getAvailableIds(std::unordered_set<common::Id>* result) const {
  CHECK_NOTNULL(result)->clear();
  std::vector<common::Id> id_vector;
  chunk_.constData()->getAvailableIds(view_time_, &id_vector);
  for (const common::Id& id : id_vector) {
    result->emplace(id);
  }
}

void ChunkView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  for (UpdateTimes::iterator it = update_times->begin();
       it != update_times->end();) {
    if (it->second <= view_time_) {
      it = update_times->erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace internal
}  // namespace map_api
