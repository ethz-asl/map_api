#ifndef MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_
#define MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_

#include "multiagent-mapping-common/reader-writer-lock.h"

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

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_INL_H_
