#include "map-api/internal/commit-future.h"

#include "map-api/chunk-transaction.h"

namespace map_api {
namespace internal {

CommitFuture::CommitFuture(
    const ChunkTransaction& finalized_committing_transaction)
    : finalized_committing_transaction_(finalized_committing_transaction) {
  CHECK(finalized_committing_transaction.isFinalized());
}

CommitFuture::~CommitFuture() {}

bool CommitFuture::has(const common::Id& id) const {
  return static_cast<bool>(get(id));
}

std::shared_ptr<const Revision> CommitFuture::get(const common::Id& id) const {
  return finalized_committing_transaction_.getById(id);
}

void CommitFuture::dump(ConstRevisionMap* result) const {
  finalized_committing_transaction_.dumpChunk(result);
}

void CommitFuture::getAvailableIds(std::unordered_set<common::Id>* result)
    const {
  finalized_committing_transaction_.getAvailableIds(result);
}

}  // namespace internal
}  // namespace map_api
