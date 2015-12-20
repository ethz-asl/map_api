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

}  // namespace internal
}  // namespace map_api
