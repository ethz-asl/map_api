#ifndef INTERNAL_COMMIT_FUTURE_H_
#define INTERNAL_COMMIT_FUTURE_H_

#include "map-api/internal/overriding-view-base.h"
#include "map-api/revision-map.h"

namespace map_api {
class ChunkTransaction;

namespace internal {

// Can replace a ChunkView in a transaction to signify that that transaction
// depends on another transaction that is committing in parallel.
class CommitFuture : public ViewBase {
 public:
  explicit CommitFuture(
      const ChunkTransaction& finalized_committing_transaction);
  explicit CommitFuture(const CommitFuture& other);
  ~CommitFuture();

  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const map_api_common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const map_api_common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<map_api_common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  ConstRevisionMap chunk_state_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMMIT_FUTURE_H_
