#include "map-api/cru-table-ram-map.h"

namespace map_api {

CRUTableRamMap::~CRUTableRamMap() {}

bool CRUTableRamMap::initCRDerived() { return true; }

bool CRUTableRamMap::insertCRUDerived(Revision* query) {
  CHECK_NOTNULL(query);
  Id id = query->getId();
  HistoryMap::iterator found = data_.find(id);
  if (found != data_.end()) {
    return false;
  }
  data_[id].push_front(*query);
  return true;
}

bool CRUTableRamMap::bulkInsertCRUDerived(const RevisionMap& query) {
  for (const RevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  for (const RevisionMap::value_type& pair : query) {
    data_[pair.first].push_front(*pair.second);
  }
  return true;
}

bool CRUTableRamMap::patchCRDerived(const Revision& query) {
  Id id = query.getId();
  LogicalTime time = query.getUpdateTime();
  HistoryMap::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, History())).first;
  }
  for (History::iterator it = found->second.begin(); it != found->second.end();
      ++it) {
    if (it->getUpdateTime() < time) {
      found->second.insert(it, query);
      return true;
    }
    LOG(WARNING) << "Patching, not in front!";  // shouldn't usually be the case
  }
  found->second.push_back(query);
  return true;
}

int CRUTableRamMap::findByRevisionCRDerived(int key,
                                            const Revision& value_holder,
                                            const LogicalTime& time,
                                            RevisionMap* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  forEachItemFoundAtTime(
      key, value_holder, time,
      [&dest](const Id& id, const History::const_iterator& item) {
    CHECK(dest->find(id) == dest->end());
    CHECK(dest->emplace(id, std::make_shared<Revision>(*item)).second);
      });
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

void CRUTableRamMap::getAvailableIdsCRDerived(const LogicalTime& time,
                                              std::unordered_set<Id>* ids) {
  CHECK_NOTNULL(ids);
  ids->clear();
  ids->rehash(data_.size());
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (!latest->isRemoved()) {
        ids->insert(pair.first);
      }
    }
  }
}

int CRUTableRamMap::countByRevisionCRDerived(int key,
                                             const Revision& value_holder,
                                             const LogicalTime& time) {
  int count = 0;
  forEachItemFoundAtTime(
      key, value_holder, time,
      [&count](const Id& /*id*/,
               const History::const_iterator& /*item*/) { ++count; });
  return count;
}

bool CRUTableRamMap::insertUpdatedCRUDerived(const Revision& query) {
  return patchCRDerived(query);
}

void CRUTableRamMap::findHistoryByRevisionCRUDerived(
    int key, const Revision& valueHolder, const LogicalTime& time,
    HistoryMap* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  // copy over history
  for (const HistoryMap::value_type& pair : data_) {
    // using current state for filter
    if (key < 0 || valueHolder.fieldMatch(*pair.second.begin(), key)) {
      CHECK(dest->insert(pair).second);
    }
  }
  // trim to time
  for (HistoryMap::value_type& pair : *dest) {
    pair.second.remove_if([&time](const Revision& item) {
      return item.getUpdateTime() > time;
    });
  }
}

inline void CRUTableRamMap::forEachItemFoundAtTime(
    int key, const Revision& value_holder, const LogicalTime& time,
    const std::function<
        void(const Id& id, const History::const_iterator& item)>& action) {
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (key < 0 || value_holder.fieldMatch(*latest, key)) {
        if (!latest->isRemoved()) {
          action(pair.first, latest);
        }
      }
    }
  }
}

} /* namespace map_api */
