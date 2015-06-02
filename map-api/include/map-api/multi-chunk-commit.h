#ifndef MAP_API_MULTI_CHUNK_COMMIT_H_
#define MAP_API_MULTI_CHUNK_COMMIT_H_

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include <multiagent-mapping-common/unique-id.h>
#include <multiagent-mapping-common/reader-writer-lock.h>

#include "./raft.pb.h"
#include "map-api/peer-id.h"

namespace map_api {
class Message;

class MultiChunkTransaction {
  friend class RaftNode;

 public:
  static const char kIsReadyToCommit[];
  static const char kCommitNotification[];
  static const char kAbortNotification[];

 private:
  enum class State {
    INACTIVE,
    LOCKED,
    RECEIVED_ALL_ENTRIES,
    AWAIT_COMMIT,
    COMMITTED,
    ABORTED
  };
  enum class OtherChunkStatus {
    READY,
    NOT_READY,
    UNKNOWN
  };
  enum class NotificationMode {
    SILENT,
    NOTIFY
  };

  explicit MultiChunkTransaction(const common::Id& id);
  void initMultiChunkTransaction(
      const proto::MultiChunkTransactionInfo multi_chunk_data,
      uint num_entries);
  void clearMultiChunkTransaction();

  void notifyReceivedRevisionIfActive();
  void notifyUnlockAndCommitReceived();
  // When unlock is not received but commit because other chunks are ready.
  void notifyProceedCommit(NotificationMode mode);
  void notifyCommitSuccess();
  void notifyAbort(NotificationMode mode);

  bool isActive();
  bool isAborted();

  bool isReadyToCommit();
  bool areAllOtherChunksReadyToCommit(std::unique_lock<std::mutex>* lock);
  bool isTransactionCommitted(const common::Id& commit_id);

  void sendQueryReadyToCommit(
      const std::unordered_set<common::Id>& ready_chunks);
  void sendCommitNotification();
  void sendAbortNotification();
  template <const char* message_type>
  bool sendMessage(const common::Id& chunk_id,
                   const proto::MultiChunkTransactionQuery& query);

  void handleQueryReadyToCommit(const proto::MultiChunkTransactionQuery& query,
                                const PeerId& sender, Message* response);
  void handleCommitNotification(const proto::MultiChunkTransactionQuery& query,
                                const PeerId& sender, Message* response);
  void handleAbortNotification(const proto::MultiChunkTransactionQuery& query,
                               const PeerId& sender, Message* response);

  void addOtherChunkStatusLocked(const common::Id& id, bool is_ready_to_commit);
  bool isChunkReadyToCommitLocked(const common::Id& id);
  inline void setStateAwaitCommitLocked() {
    state_ = State::AWAIT_COMMIT;
    CHECK(older_commits_.insert(current_transaction_id_).second);
  }

  const common::Id my_chunk_id_;

  // State during a transaction.
  State state_;
  common::Id current_transaction_id_;
  uint num_commits_received_;
  uint num_revision_entries_;
  std::unordered_map<common::Id, OtherChunkStatus> other_chunk_status_;
  std::unordered_set<common::Id> older_commits_;
  // Don't send notification RPCs when committing on a new joining peer.
  bool notifications_enable_;
  std::mutex mutex_;

  // TODO(aqurai): To be removed. (Issue #2466)
  std::unordered_map<common::Id, PeerId> other_chunk_leaders_;
  const proto::MultiChunkTransactionInfo* multi_chunk_data_;
  common::ReaderWriterMutex data_mutex_;
};

}  // namespace map_api

#endif  // MAP_API_MULTI_CHUNK_COMMIT_H_
