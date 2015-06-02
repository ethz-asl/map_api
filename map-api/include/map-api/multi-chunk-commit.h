#ifndef MAP_API_MULTI_CHUNK_COMMIT_H_
#define MAP_API_MULTI_CHUNK_COMMIT_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <multiagent-mapping-common/unique-id.h>

#include "./raft.pb.h"

namespace map_api {
class Message;
class PeerId;

class MultiChunkCommit {
  friend class RaftNode;

 public:
  static const char kIsReadyToCommit[];
  // static const char kIsReadyResponse[];
  static const char kCommitNotification[];
  static const char kAbortNotification[];

 private:
  enum class State {
    INACTIVE,
    LOCKED,
    READY_TO_COMMIT,
    AWAIT_COMMIT,
    COMMITTED,
    ABORTED
  };
  enum class OtherChunkStatus {
    READY,
    NOT_READY,
    UNKNOWN
  };

  explicit MultiChunkCommit(const common::Id& id);
  void initMultiChunkCommit(const proto::MultiChunkCommitInfo multi_chunk_data,
                            uint num_entries);
  void clearMultiChunkCommit();

  void notifyReceivedRevisionIfActive();
  void noitfyCommitBegin();
  void notifyCommitSuccess();
  void notifyAbort();

  bool isActive();

  bool isReadyToCommit();
  bool areAllOtherChunksReadyToCommit();
  bool isTransactionCommitted(const common::Id& commit_id);

  void sendQueryReadyToCommit();
  void sendCommitNotification();
  void sendAbortNotification();

  void handleQueryReadyToCommit(const proto::MultiChunkCommitQuery& query,
                                const PeerId& sender, Message* response);
  void handleCommitNotification(const proto::MultiChunkCommitQuery& query,
                                const PeerId& sender, Message* response);
  void handleAbortNotification(const proto::MultiChunkCommitQuery& query,
                               const PeerId& sender, Message* response);

  template <const char* message_type>
  bool sendMessage(const common::Id& id, const proto::MultiChunkCommitQuery& query);

  void fetchOtherChunkStatusLocked();
  void addOtherChunkStatusLocked(const common::Id& id, bool is_ready_to_commit);

  common::Id my_chunk_id_;

  // State during a transaction.
  State state_;
  common::Id current_transaction_id_;
  uint num_commits_received_;
  uint num_revision_entries_;
  const proto::MultiChunkCommitInfo* multi_chunk_data_;
  std::unordered_map<common::Id, OtherChunkStatus> other_chunk_status_;
  bool asked_all_;

  std::unordered_set<common::Id> older_commits_;
  // Don't send notification RPCs when committing on a new joining peer.
  bool notifications_enable_;
  std::mutex mutex_;
};

}  // namespace map_api

#endif  // MAP_API_MULTI_CHUNK_COMMIT_H_
