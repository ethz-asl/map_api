#include "../include/map-api/chunk-data-ram-container.h"

namespace map_api {

ChunkDataRamContainer::~ChunkDataRamContainer() {}

bool ChunkDataRamContainer::initImpl() { return true; }

bool ChunkDataRamContainer::insertImpl(
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

bool ChunkDataRamContainer::bulkInsertImpl(const MutableRevisionMap& query) {
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

bool ChunkDataRamContainer::patchImpl(
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

std::shared_ptr<const Revision> ChunkDataRamContainer::getByIdImpl(
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

void ChunkDataRamContainer::findByRevisionImpl(int key,
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

void ChunkDataRamContainer::getAvailableIdsImpl(
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

int ChunkDataRamContainer::countByRevisionImpl(int key,
                                               const Revision& value_holder,
                                               const LogicalTime& time) const {
  int count = 0;
  forEachItemFoundAtTime(key, value_holder, time,
                         [&count](const common::Id& /*id*/,
                                  const Revision& /*item*/) { ++count; });
  return count;
}

bool ChunkDataRamContainer::insertUpdatedImpl(
    const std::shared_ptr<Revision>& query) {
  return patchImpl(query);
}

void ChunkDataRamContainer::findHistoryByRevisionImpl(
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

void ChunkDataRamContainer::chunkHistory(const common::Id& chunk_id,
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

void ChunkDataRamContainer::itemHistoryImpl(const common::Id& id,
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

void ChunkDataRamContainer::clearImpl() { data_.clear(); }

inline void ChunkDataRamContainer::forEachItemFoundAtTime(
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

inline void ChunkDataRamContainer::forChunkItemsAtTime(
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

inline void ChunkDataRamContainer::trimToTime(const LogicalTime& time,
                                              HistoryMap* subject) const {
  CHECK_NOTNULL(subject);
  for (HistoryMap::value_type& pair : *subject) {
    pair.second.remove_if([&time](const std::shared_ptr<const Revision>& item) {
      return item->getUpdateTime() > time;
    });
  }
}

} /* namespace map_api */
