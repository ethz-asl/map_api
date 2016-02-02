#ifndef INTERNAL_DELTA_VIEW_H_
#define INTERNAL_DELTA_VIEW_H_

#include "map-api/internal/overriding-view-base.h"
#include "map-api/revision-map.h"

namespace map_api {
class ChunkBase;
class ChunkTransaction;
class Conflicts;

namespace internal {

class DeltaView : public OverridingViewBase {
 public:
  explicit DeltaView(const NetTable& table);
  ~DeltaView();

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
  virtual bool suppresses(const common::Id& id) const override;

  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);

  // Returns false if no update entry is available; asserts that the id hasn't
  // been removed.
  bool getMutableUpdateEntry(const common::Id& id,
                             std::shared_ptr<const Revision>** result);

  bool hasConflictsAfterTryingToMerge(
      const std::unordered_map<common::Id, LogicalTime>& potential_conflicts,
      const ViewBase& original_view, const ViewBase& conflict_view);

  // Asserts that the chunk is locked.
  void checkedCommitLocked(
      const LogicalTime& commit_time, ChunkBase* locked_chunk,
      std::unordered_map<common::Id, LogicalTime>* commit_log);

  void prepareManualMerge(
      const std::unordered_map<common::Id, LogicalTime>& potential_conflicts,
      const ViewBase& original_view, const ViewBase& conflict_view,
      DeltaView* conflict_free_part, Conflicts* conflicts);

  size_t numChanges() const;

 private:
  // Strong typing of operation maps.
  class RevisionEventMap : public MutableRevisionMap {
   public:
    void logCommitEvent(
        const LogicalTime& commit_time,
        std::unordered_map<common::Id, LogicalTime>* commit_history) const;
  };

  class InsertMap : public RevisionEventMap {};
  class UpdateMap : public RevisionEventMap {};
  class RemoveMap : public RevisionEventMap {};

  enum class ConflictTraversalMode {
    kTryMergeOrBail,
    kPrepareManualMerge
  };

  bool traverseConflicts(
      const ConflictTraversalMode mode,
      const std::unordered_map<common::Id, LogicalTime>& potential_conflicts,
      const ViewBase& original_view, const ViewBase& conflict_view,
      DeltaView* conflict_free_part, Conflicts* conflicts);

  bool tryAutoMerge(const ViewBase& original_view,
                    const ViewBase& conflict_view,
                    UpdateMap::value_type* item) const;

  // What the delta consists of. This class guarantees that ids are unique
  // across all maps, e.g. an inserted id will never also be removed.
  InsertMap insertions_;
  UpdateMap updates_;
  RemoveMap removes_;
  friend class ::map_api::ChunkTransaction;  // TODO(tcies) full split.

  // For debug printing and merge policies.
  const NetTable& table_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_DELTA_VIEW_H_
