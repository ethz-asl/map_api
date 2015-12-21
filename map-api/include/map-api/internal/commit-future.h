#ifndef INTERNAL_COMMIT_FUTURE_H_
#define INTERNAL_COMMIT_FUTURE_H_

#include "map-api/internal/overriding-view-base.h"

namespace map_api {
namespace internal {

// Can replace a ChunkView in a transaction to signify that that transaction
// depends on another transaction that is committing in parallel.
class CommitFuture : public ViewBase {
 public:
  explicit CommitFuture(
      const ChunkTransaction& finalized_committing_transaction);
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

 private:
  const ChunkTransaction& finalized_committing_transaction_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMMIT_FUTURE_H_
