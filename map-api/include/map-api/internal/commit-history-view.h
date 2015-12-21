#ifndef INTERNAL_COMMIT_HISTORY_VIEW_H_
#define INTERNAL_COMMIT_HISTORY_VIEW_H_

#include <unordered_map>

#include "map-api/internal/overriding-view-base.h"

namespace map_api {
class ChunkBase;
class LogicalTime;

namespace internal {

class CommitHistoryView : public OverridingViewBase {
 public:
  typedef std::unordered_map<common::Id, LogicalTime> History;

  CommitHistoryView(const History& commit_history, const ChunkBase& chunk);
  ~CommitHistoryView();

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
  // ============================
  // OVERRIDINGVIEWBASE INTERFACE
  // ============================
  virtual bool supresses(const common::Id& id) const override;

 private:
  const std::unordered_map<common::Id, LogicalTime>& commit_history_;
  const ChunkBase& chunk_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMMIT_HISTORY_VIEW_H_
