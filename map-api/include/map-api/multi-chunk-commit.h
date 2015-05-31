#ifndef MAP_API_MULTI_CHUNK_COMMIT_H_
#define MAP_API_MULTI_CHUNK_COMMIT_H_

#include <memory>
#include <unordered_map>

#include <multiagent-mapping-common/unique-id.h>

#include "./raft.pb.h"

namespace map_api {

class MultiChunkCommit {
  friend class RaftNode;

 private:
  enum class State {
    INACTIVE,
    LOCKED,
    READY_TO_COMMIT,
    AWAIT_COMMIT,
    COMMITTED,
    ABORTING
  };
  MultiChunkCommit();
  ~MultiChunkCommit();

  void initMultiChunkCommit(
      std::shared_ptr<const proto::MultiChunkCommitInfo> multi_chunk_data);

  void clearMultiChunkCommit();

  void fetchOtherChunkStatus();

  std::unordered_map<common::Id, bool> other_chunk_status_;
  std::unordered_map<common::Id, bool> older_commit_status_;
  std::shared_ptr<const proto::MultiChunkCommitInfo> multi_chunk_data_;
  std::mutex mutex_;
};

}  // namespace map_api

#endif  // MAP_API_MULTI_CHUNK_COMMIT_H_
