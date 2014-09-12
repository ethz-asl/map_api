#include "map-api/cru-table-ram-map.h"

namespace map_api {

CRUTableRamMap::~CRUTableRamMap() {}

bool CRUTableRamMap::initCRDerived() { return true; }

bool CRUTableRamMap::insertCRUDerived(Revision* query) {
  CHECK_NOTNULL(query);
  Id id = query->getId<Id>();
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
  Id id = query.getId<Id>();
  LogicalTime time = query.getUpdateTime();
  HistoryMap::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, History())).first;
  }
  for (History::iterator it = found->second.begin(); it != found->second.end();
      ++it) {
    if (it->getUpdateTime() <= time) {
      CHECK_NE(time, it->getUpdateTime());
      found->second.insert(it, query);
      return true;
    }
    LOG(WARNING) << "Patching, not in front!";  // shouldn't usually be the case
  }
  found->second.push_back(query);
  return true;
}

std::shared_ptr<Revision> CRUTableRamMap::getByIdCRDerived(
    const Id& id, const LogicalTime& time) const {
  HistoryMap::const_iterator found = data_.find(id);
  if (found == data_.end()) {
    return std::shared_ptr<Revision>();
  }
  History::const_iterator latest = found->second.latestAt(time);
  if (latest == found->second.end()) {
    return std::shared_ptr<Revision>();
  }
  return std::make_shared<Revision>(*latest);
}

void CRUTableRamMap::dumpChunkCRDerived(const Id& chunk_id,
                                        const LogicalTime& time,
                                        RevisionMap* dest) {
  CHECK_NOTNULL(dest)->clear();
  forChunkItemsAtTime(
      chunk_id, time,
      [&dest](const Id& id, const History::const_iterator& item) {
        CHECK(dest->emplace(id, std::make_shared<Revision>(*item)).second);
      });
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

int CRUTableRamMap::countByChunkCRDerived(const Id& chunk_id,
                                          const LogicalTime& time) {
  int count = 0;
  forChunkItemsAtTime(
      chunk_id, time,
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
  for (const HistoryMap::value_type& pair : data_) {
    // using current state for filter
    if (key < 0 || valueHolder.fieldMatch(*pair.second.begin(), key)) {
      CHECK(dest->insert(pair).second);
    }
  }
  trimToTime(time, dest);
}

void CRUTableRamMap::chunkHistory(const Id& chunk_id, const LogicalTime& time,
                                  HistoryMap* dest) {
  CHECK_NOTNULL(dest)->clear();
  for (const HistoryMap::value_type& pair : data_) {
    if (pair.second.begin()->getChunkId() == chunk_id) {
      CHECK(dest->emplace(pair).second);
    }
  }
  trimToTime(time, dest);
}

void CRUTableRamMap::itemHistoryCRUDerived(const Id& id,
                                           const LogicalTime& time,
                                           History* dest) {
  CHECK_NOTNULL(dest)->clear();
  HistoryMap::const_iterator found = data_.find(id);
  CHECK(found != data_.end());
  *dest = History(found->second);
  dest->remove_if([&time](const Revision& item) {
    return item.getUpdateTime() > time;
  });
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

inline void CRUTableRamMap::forChunkItemsAtTime(
    const Id& chunk_id, const LogicalTime& time,
    const std::function<
        void(const Id& id, const History::const_iterator& item)>& action) {
  for (const HistoryMap::value_type& pair : data_) {
    if (pair.second.begin()->getChunkId() == chunk_id) {
      History::const_iterator latest = pair.second.latestAt(time);
      if (latest != pair.second.cend()) {
        if (!latest->isRemoved()) {
          action(pair.first, latest);
        }
      }
    }
  }
}

inline void CRUTableRamMap::trimToTime(const LogicalTime& time,
                                       HistoryMap* subject) {
  CHECK_NOTNULL(subject);
  for (HistoryMap::value_type& pair : *subject) {
    pair.second.remove_if([&time](const Revision& item) {
      return item.getUpdateTime() > time;
    });
  }
}

} /* namespace map_api */
