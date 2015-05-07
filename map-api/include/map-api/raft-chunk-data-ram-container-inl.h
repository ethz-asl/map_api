#ifndef MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_
#define MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_

#include <stdint.h>
#include <mutex>

namespace map_api {

RaftChunkDataRamContainer::History::const_iterator
RaftChunkDataRamContainer::History::latestAt(const LogicalTime& time) const {
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    if ((*it)->getUpdateTime() <= time) {
      return it;
    }
  }
  return cend();
}

void RaftChunkDataRamContainer::forEachItemFoundAtTime(
    int key, const Revision& value_holder, const LogicalTime& time,
    const std::function<void(const common::Id& id,
                             const Revision::ConstPtr& item)>& action) const {
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (key < 0 || value_holder.fieldMatch(**latest, key)) {
        if (!(*latest)->isRemoved()) {
          action(pair.first, *latest);
        }
      }
    }
  }
}

void RaftChunkDataRamContainer::forChunkItemsAtTime(
    const common::Id& chunk_id, const LogicalTime& time,
    const std::function<void(
        const common::Id& id,
        const Revision::ConstPtr& item)>& action) const {
  for (const HistoryMap::value_type& pair : data_) {
    if ((*pair.second.begin())->getChunkId() == chunk_id) {
      History::const_iterator latest = pair.second.latestAt(time);
      if (latest != pair.second.cend()) {
        if (!(*latest)->isRemoved()) {
          action(pair.first, *latest);
        }
      }
    }
  }
}

void RaftChunkDataRamContainer::trimToTime(const LogicalTime& time,
                                           HistoryMap* subject) const {
  CHECK_NOTNULL(subject);
  for (HistoryMap::value_type& pair : *subject) {
    pair.second.remove_if([&time](const Revision::ConstPtr& item) {
      return item->getUpdateTime() > time;
    });
  }
}

inline uint64_t RaftChunkDataRamContainer::logCommitIndex() const {
  LogReadAccess log_reader(this);
  return log_reader->commitIndex();
  // return log_.commit_index_;
}

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_
