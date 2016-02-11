#ifndef INTERNAL_COMMIT_FUTURE_H_
#define INTERNAL_COMMIT_FUTURE_H_

#include "dmap/internal/overriding-view-base.h"
#include "dmap/revision-map.h"

namespace dmap {
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
  virtual bool has(const common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  ConstRevisionMap chunk_state_;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_COMMIT_FUTURE_H_
