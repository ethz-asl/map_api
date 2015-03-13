#include <map-api/table-data-ram-container.h>

namespace map_api {

TableDataRamContainer::~TableDataRamContainer() {}

bool TableDataRamContainer::initImpl() { return true; }

bool TableDataRamContainer::insertImpl(
    const std::shared_ptr<const Revision>& query) {
  CHECK(query != nullptr);
  common::Id id = query->getId<common::Id>();
  HistoryMap::iterator found = data_.find(id);
  if (found != data_.end()) {
    return false;
  }
  data_[id].push_front(query);
  return true;
}

bool TableDataRamContainer::bulkInsertImpl(const MutableRevisionMap& query) {
  for (const MutableRevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  for (const MutableRevisionMap::value_type& pair : query) {
    data_[pair.first].push_front(pair.second);
  }
  return true;
}

bool TableDataRamContainer::patchImpl(
    const std::shared_ptr<const Revision>& query) {
  CHECK(query != nullptr);
  common::Id id = query->getId<common::Id>();
  LogicalTime time = query->getUpdateTime();
  HistoryMap::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, History())).first;
  }
  for (History::iterator it = found->second.begin(); it != found->second.end();
       ++it) {
    if ((*it)->getUpdateTime() <= time) {
      CHECK_NE(time, (*it)->getUpdateTime());
      found->second.insert(it, query);
      return true;
    }
    LOG(WARNING) << "Patching, not in front!";  // shouldn't usually be the case
  }
  found->second.push_back(query);
  return true;
}

std::shared_ptr<const Revision> TableDataRamContainer::getByIdImpl(
    const common::Id& id, const LogicalTime& time) const {
  HistoryMap::const_iterator found = data_.find(id);
  if (found == data_.end()) {
    return std::shared_ptr<Revision>();
  }
  History::const_iterator latest = found->second.latestAt(time);
  if (latest == found->second.end()) {
    return std::shared_ptr<Revision>();
  }
  return *latest;
}

void TableDataRamContainer::dumpChunkImpl(const common::Id& chunk_id,
                                          const LogicalTime& time,
                                          ConstRevisionMap* dest) const {
  CHECK_NOTNULL(dest)->clear();
  // TODO(tcies) Zero-copy const RevisionMap instead of copyForWrite?
  forChunkItemsAtTime(chunk_id, time,
                      [&dest](const common::Id& id, const Revision& item) {
    CHECK(dest->emplace(id, item.copyForWrite()).second);
  });
}

void TableDataRamContainer::findByRevisionImpl(int key,
                                               const Revision& value_holder,
                                               const LogicalTime& time,
                                               ConstRevisionMap* dest) const {
  CHECK_NOTNULL(dest);
  dest->clear();
  // TODO(tcies) Zero-copy const RevisionMap instead of copyForWrite?
  forEachItemFoundAtTime(key, value_holder, time,
                         [&dest](const common::Id& id, const Revision& item) {
    CHECK(dest->find(id) == dest->end());
    CHECK(dest->emplace(id, item.copyForWrite()).second);
  });
}

void TableDataRamContainer::getAvailableIdsImpl(
    const LogicalTime& time, std::vector<common::Id>* ids) const {
  CHECK_NOTNULL(ids);
  ids->clear();
  ids->reserve(data_.size());
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (!(*latest)->isRemoved()) {
        ids->emplace_back(pair.first);
      }
    }
  }
}

int TableDataRamContainer::countByRevisionImpl(int key,
                                               const Revision& value_holder,
                                               const LogicalTime& time) const {
  int count = 0;
  forEachItemFoundAtTime(key, value_holder, time,
                         [&count](const common::Id& /*id*/,
                                  const Revision& /*item*/) { ++count; });
  return count;
}

int TableDataRamContainer::countByChunkImpl(const common::Id& chunk_id,
                                            const LogicalTime& time) const {
  int count = 0;
  forChunkItemsAtTime(chunk_id, time,
                      [&count](const common::Id& /*id*/,
                               const Revision& /*item*/) { ++count; });
  return count;
}

bool TableDataRamContainer::insertUpdatedImpl(
    const std::shared_ptr<Revision>& query) {
  return patchImpl(query);
}

void TableDataRamContainer::findHistoryByRevisionImpl(
    int key, const Revision& valueHolder, const LogicalTime& time,
    HistoryMap* dest) const {
  CHECK_NOTNULL(dest);
  dest->clear();
  for (const HistoryMap::value_type& pair : data_) {
    // using current state for filter
    if (key < 0 || valueHolder.fieldMatch(**pair.second.begin(), key)) {
      CHECK(dest->insert(pair).second);
    }
  }
  trimToTime(time, dest);
}

void TableDataRamContainer::chunkHistory(const common::Id& chunk_id,
                                         const LogicalTime& time,
                                         HistoryMap* dest) const {
  CHECK_NOTNULL(dest)->clear();
  for (const HistoryMap::value_type& pair : data_) {
    if ((*pair.second.begin())->getChunkId() == chunk_id) {
      CHECK(dest->emplace(pair).second);
    }
  }
  trimToTime(time, dest);
}

void TableDataRamContainer::itemHistoryImpl(const common::Id& id,
                                            const LogicalTime& time,
                                            History* dest) const {
  CHECK_NOTNULL(dest)->clear();
  HistoryMap::const_iterator found = data_.find(id);
  CHECK(found != data_.end());
  *dest = History(found->second);
  dest->remove_if([&time](const std::shared_ptr<const Revision>& item) {
    return item->getUpdateTime() > time;
  });
}

void TableDataRamContainer::clearImpl() { data_.clear(); }

inline void TableDataRamContainer::forEachItemFoundAtTime(
    int key, const Revision& value_holder, const LogicalTime& time,
    const std::function<void(const common::Id& id, const Revision& item)>&
        action) const {
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (key < 0 || value_holder.fieldMatch(**latest, key)) {
        if (!(*latest)->isRemoved()) {
          action(pair.first, **latest);
        }
      }
    }
  }
}

inline void TableDataRamContainer::forChunkItemsAtTime(
    const common::Id& chunk_id, const LogicalTime& time,
    const std::function<void(const common::Id& id, const Revision& item)>&
        action) const {
  for (const HistoryMap::value_type& pair : data_) {
    if ((*pair.second.begin())->getChunkId() == chunk_id) {
      History::const_iterator latest = pair.second.latestAt(time);
      if (latest != pair.second.cend()) {
        if (!(*latest)->isRemoved()) {
          action(pair.first, **latest);
        }
      }
    }
  }
}

inline void TableDataRamContainer::trimToTime(const LogicalTime& time,
                                              HistoryMap* subject) const {
  CHECK_NOTNULL(subject);
  for (HistoryMap::value_type& pair : *subject) {
    pair.second.remove_if([&time](const std::shared_ptr<const Revision>& item) {
      return item->getUpdateTime() > time;
    });
  }
}

} /* namespace map_api */
