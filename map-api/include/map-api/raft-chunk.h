#ifndef MAP_API_RAFT_CHUNK_H_
#define MAP_API_RAFT_CHUNK_H_

#include <mutex>
#include <set>

#include <multiagent-mapping-common/condition.h>
#include <multiagent-mapping-common/unique-id.h>

#include "./chunk.pb.h"
#include "map-api/chunk-base.h"
#include "map-api/raft-node.h"

namespace map_api {
class Message;
class Revision;

class RaftChunk : public ChunkBase {
  friend class ChunkTransaction;
  friend class ConsensusFixture;
  FRIEND_TEST(ConsensusFixture, LeaderElection);

 public:
  RaftChunk();
  virtual ~RaftChunk();

  bool init(const common::Id& id, std::shared_ptr<TableDescriptor> descriptor,
            bool initialize);
  virtual void initializeNewImpl(
      const common::Id& id,
      const std::shared_ptr<TableDescriptor>& descriptor) override;
  bool init(const common::Id& id, const proto::InitRequest& init_request,
            std::shared_ptr<TableDescriptor> descriptor);
  virtual void dumpItems(const LogicalTime& time, ConstRevisionMap* items) const
      override;
  inline void setStateFollowerAndStartRaft();
  inline void setStateLeaderAndStartRaft();

  virtual size_t numItems(const LogicalTime& time) const override;
  virtual size_t itemsSizeBytes(const LogicalTime& time) const override;
  virtual void getCommitTimes(const LogicalTime& sample_time,
                              std::set<LogicalTime>* commit_times) const override;

  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& item) override;

  inline virtual int peerSize() const override;

  // Mutable because the method declarations in base class are const.
  mutable bool chunk_lock_attempted_;
  mutable bool is_raft_chunk_locked_;
  mutable uint64_t lock_log_index_;
  mutable int chunk_write_lock_depth_;
  mutable int self_read_lock_depth_;
  mutable std::condition_variable chunk_lock_cv_;
  mutable std::mutex write_lock_mutex_;
  virtual void writeLock() override;
  virtual void readLock() const override;  // No read lock for raft chunks.
  virtual bool isWriteLocked() override;
  virtual void unlock() const override;

  virtual int requestParticipation() override;
  virtual int requestParticipation(const PeerId& peer) override;

  virtual void update(const std::shared_ptr<Revision>& item) override;

  static bool sendConnectRequest(const PeerId& peer,
                                 proto::ChunkRequestMetadata& metadata);

  virtual LogicalTime getLatestCommitTime() const override {
    LOG(WARNING) << "RaftChunk::insert() is not implemented";
    return LogicalTime::sample();
  }

 private:
  virtual void bulkInsertLocked(const MutableRevisionMap& items,
                                const LogicalTime& time) override;
  virtual void updateLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override;
  virtual void removeLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override;

  inline void syncLatestCommitTime(const Revision& item);

  uint64_t raftInsertRequest(const Revision::ConstPtr& item);

  virtual void leaveImpl() override;
  virtual void awaitShared() override;

  class ChunkRequestId {
   public:
    ChunkRequestId() : serial_id_(0) {}
    inline uint64_t getNewId() { return ++serial_id_; }

   private:
    std::atomic<uint64_t> serial_id_;
  };
  mutable ChunkRequestId request_id_;

  /**
   * ==========================================
   * Handlers for RPCs addressed to this Chunk.
   * ==========================================
   */
  friend class NetTable;

  // Chunk Requests.
  inline void handleRaftConnectRequest(const PeerId& sender, Message* response);
  inline void handleRaftLeaveRequest(const PeerId& sender, uint64_t serial_id,
                                     Message* response);
  void handleRaftLeaveNotification(Message* response);
  inline void handleRaftChunkLockRequest(const PeerId& sender,
                                         uint64_t serial_id, Message* response);
  inline void handleRaftChunkUnlockRequest(const PeerId& sender,
                                           uint64_t serial_id,
                                           uint64_t lock_index,
                                           bool proceed_commits,
                                           Message* response);
  inline void handleRaftInsertRequest(proto::InsertRequest* request,
                                      const PeerId& sender, Message* response);

  // Raft Requests.
  inline void handleRaftAppendRequest(proto::AppendEntriesRequest* request,
                                      const PeerId& sender, Message* response);
  inline void handleRaftRequestVote(const proto::VoteRequest& request,
                                    const PeerId& sender, Message* response);
  inline void handleRaftQueryState(const proto::QueryState& request,
                                   Message* response);

  // Leaving the chunk.
  bool leave_requested_;
  common::Condition leave_notification_;

  // Handles all communication with other chunk holders. No communication except
  // for peer join shall happen between chunk holder peers outside of raft.
  // TODO(aqurai): Making this mutable only because unlock() is const. Remove
  // const qualifier for unlock() in base chunk and other derived chunks
  // (#2436).
  mutable RaftNode raft_node_;
  volatile bool initialized_ = false;
  volatile bool relinquished_ = false;
  LogicalTime latest_commit_time_;
  uint64_t latest_commit_log_index_;
};

}  // namespace map_api

#include "./raft-chunk-inl.h"

#endif  // MAP_API_RAFT_CHUNK_H_
