// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

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
  virtual bool has(const map_api_common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const map_api_common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<map_api_common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

  // ============================
  // OVERRIDINGVIEWBASE INTERFACE
  // ============================
  virtual bool suppresses(const map_api_common::Id& id) const override;

  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);

  // Returns false if no update entry is available; asserts that the id hasn't
  // been removed.
  bool getMutableUpdateEntry(const map_api_common::Id& id,
                             std::shared_ptr<const Revision>** result);

  bool hasConflictsAfterTryingToMerge(
      const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
      const ViewBase& original_view, const ViewBase& conflict_view);

  // Asserts that the chunk is locked.
  void checkedCommitLocked(
      const LogicalTime& commit_time, ChunkBase* locked_chunk,
      std::unordered_map<map_api_common::Id, LogicalTime>* commit_log);

  void prepareManualMerge(
      const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
      const ViewBase& original_view, const ViewBase& conflict_view,
      DeltaView* conflict_free_part, Conflicts* conflicts);

  size_t numChanges() const;

 private:
  // Strong typing of operation maps.
  class RevisionEventMap : public MutableRevisionMap {
   public:
    void logCommitEvent(
        const LogicalTime& commit_time,
        std::unordered_map<map_api_common::Id, LogicalTime>* commit_history) const;
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
      const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
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
