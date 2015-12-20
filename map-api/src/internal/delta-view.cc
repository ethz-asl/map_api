#include "map-api/internal/delta-view.h"

#include <multiagent-mapping-common/unique-id.h>

namespace map_api {
namespace internal {

DeltaView::~DeltaView() {}

bool DeltaView::has(const common::Id& id) const {
  return (updates_.count(id) != 0u || insertions_.count(id) != 0u) &&
         removes_.count(id) == 0u;
}

std::shared_ptr<const Revision> DeltaView::get(const common::Id& id) const {
  CHECK_EQ(removes_.count(id), 0u);
  UpdateMap::const_iterator found = updates_.find(id);
  if (found != updates_.end()) {
    return std::const_pointer_cast<const Revision>(found->second);
  } else {
    InsertMap::const_iterator found = insertions_.find(id);
    CHECK(found != insertions_.end());
    return std::const_pointer_cast<const Revision>(found->second);
  }
}

bool DeltaView::supresses(const common::Id& id) const {
  return removes_.count(id) != 0u;
}

void DeltaView::insert(std::shared_ptr<Revision> revision) {
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  CHECK_EQ(updates_.count(id), 0u);
  CHECK_EQ(removes_.count(id), 0u);
  // Check emplacement, as insertions are mutually exclusive.
  CHECK(insertions_.emplace(id, revision).second);
}

void DeltaView::update(std::shared_ptr<Revision> revision) {
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  InsertMap::iterator corresponding_insertion = insertions_.find(id);
  if (corresponding_insertion != insertions_.end()) {
    // If this updates a revision added also in this transaction, the insertion
    // is replaced with the update, in order to ensure the setting of default
    // fields such as insert time and chunk id.
    corresponding_insertion->second = revision;
  } else {
    CHECK_EQ(removes_.count(id), 0u);
    // Assignment, as later updates supersede earlier ones.
    updates_[id] = revision;
  }
}

void DeltaView::remove(std::shared_ptr<Revision> revision) {
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());

  UpdateMap::iterator corresponding_update = updates_.find(id);
  if (corresponding_update != updates_.end()) {
    updates_.erase(corresponding_update);
  }

  InsertMap::iterator corresponding_insertion = insertions_.find(id);
  if (corresponding_insertion != insertions_.end()) {
    insertions_.erase(corresponding_insertion);
  } else {
    CHECK(removes_.emplace(id, revision).second);
  }
}

}  // namespace internal
}  // namespace map_api
