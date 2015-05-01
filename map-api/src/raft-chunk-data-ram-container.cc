#include "map-api/raft-chunk-data-ram-container.h"

namespace map_api {

RaftChunkDataRamContainer::~RaftChunkDataRamContainer() {}

bool RaftChunkDataRamContainer::initImpl() { return true; }

RaftChunkDataRamContainer::RaftLog::iterator
RaftChunkDataRamContainer::RaftLog::getLogIteratorByIndex(uint64_t index) {
  iterator it = end();
  if (index < front()->index() || index > back()->index()) {
    return it;
  } else {
    // The log indices are always sequential.
    it = begin() + (index - front()->index());
    CHECK_EQ((*it)->index(), index) << " Log entries size = " << size();
    return it;
  }
}

std::shared_ptr<const Revision> RaftChunkDataRamContainer::getByIdImpl(
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

void RaftChunkDataRamContainer::findByRevisionImpl(
    int key, const Revision& value_holder, const LogicalTime& time,
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

void RaftChunkDataRamContainer::getAvailableIdsImpl(
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

int RaftChunkDataRamContainer::countByRevisionImpl(
    int key, const Revision& value_holder, const LogicalTime& time) const {
  int count = 0;
  forEachItemFoundAtTime(key, value_holder, time,
                         [&count](const common::Id& /*id*/,
                                  const Revision& /*item*/) { ++count; });
  return count;
}

inline void RaftChunkDataRamContainer::forEachItemFoundAtTime(
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

/*template <typename IdType>
void RaftChunkDataRamContainer::itemHistory(const IdType& id,
                                            const LogicalTime& time,
                                            History* dest) const {
  common::Id map_api_id;
  aslam::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  itemHistoryImpl(map_api_id, time, dest);

  CHECK_NOTNULL(dest)->clear();
  HistoryMap::const_iterator found = data_.find(map_api_id);
  CHECK(found != data_.end());
  *dest = History(found->second);
  dest->remove_if([&time](const std::shared_ptr<const Revision>& item) {
    return item->getUpdateTime() > time;
  });
}*/

uint64_t RaftChunkDataRamContainer::RaftLog::eraseAfter(iterator it) {
  CHECK(it + 1 != begin());
  resize(std::distance(begin(), it + 1));
  return lastLogIndex();
}

}  // namespace map_api
