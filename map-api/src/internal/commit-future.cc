#include "map-api/internal/commit-future.h"

#include "map-api/chunk-transaction.h"

namespace map_api {
namespace internal {

CommitFuture::CommitFuture(
    const ChunkTransaction& finalized_committing_transaction) {
  CHECK(finalized_committing_transaction.finalized_);
  const DeltaView& finalized_delta = finalized_committing_transaction.delta_;
  std::unique_ptr<ViewBase> current_view(new ChunkView(
      *finalized_committing_transaction.chunk_, LogicalTime::sample()));
  CombinedView future_view(current_view, finalized_delta);
  future_view.dump(&chunk_state_);
}

CommitFuture::CommitFuture(const CommitFuture& other)
    : chunk_state_(other.chunk_state_) {}

CommitFuture::~CommitFuture() {}

bool CommitFuture::has(const common::Id& id) const {
  return static_cast<bool>(get(id));
}

std::shared_ptr<const Revision> CommitFuture::get(const common::Id& id) const {
  ConstRevisionMap::const_iterator found = chunk_state_.find(id);
  if (found != chunk_state_.end()) {
    return found->second;
  } else {
    return std::shared_ptr<const Revision>();
  }
}

void CommitFuture::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->insert(chunk_state_.begin(), chunk_state_.end());
}

void CommitFuture::getAvailableIds(std::unordered_set<common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (const ConstRevisionMap::value_type& id_revision : chunk_state_) {
    result->emplace(id_revision.first);
  }
}

void CommitFuture::discardKnownUpdates(UpdateTimes* update_times) const {
  LOG(FATAL) << "Detach futures from chunk transaction before committing!";
}

}  // namespace internal
}  // namespace map_api
