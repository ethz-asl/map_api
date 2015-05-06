#ifndef MAP_API_RAFT_CHUNK_H_
#define MAP_API_RAFT_CHUNK_H_

#include <mutex>
#include <set>

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
  FRIEND_TEST(ConsensusFixture, RaftChunkTest);

 public:
  virtual ~RaftChunk();

  bool init(const common::Id& id, std::shared_ptr<TableDescriptor> descriptor,
            bool initialize);
  virtual void initializeNewImpl(
      const common::Id& id,
      const std::shared_ptr<TableDescriptor>& descriptor) override;
  bool init(const common::Id& id, std::shared_ptr<TableDescriptor> descriptor);
  bool init(const common::Id& id, const proto::InitRequest& init_request,
            std::shared_ptr<TableDescriptor> descriptor);
  virtual void dumpItems(const LogicalTime& time, ConstRevisionMap* items) const
      override;
  inline void setStateFollowerAndStartRaft();
  inline void setStateLeaderAndStartRaft();

  // ====================
  // Not implemented yet.
  // ====================
  virtual size_t numItems(const LogicalTime& time) const override;
  virtual size_t itemsSizeBytes(const LogicalTime& time) const override;
  virtual void getCommitTimes(const LogicalTime& sample_time,
                              std::set<LogicalTime>* commit_times) const override;
  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& item) override;
  inline virtual int peerSize() const override;

  // Mutable because the method declarations in base class are const.
  mutable bool is_raft_write_locked_;
  mutable int write_lock_depth_;
  mutable std::mutex write_lock_mutex_;
  virtual void writeLock() override;
  virtual void readLock() const override {}  // No read lock for raft chunks.
  virtual bool isWriteLocked() override;
  virtual void unlock() const override;

  virtual int requestParticipation() override;
  virtual int requestParticipation(const PeerId& peer) override;
  virtual void update(const std::shared_ptr<Revision>& item) override;
  virtual LogicalTime getLatestCommitTime() const override {return LogicalTime::sample();}

  // ========================================================================

  static bool sendConnectRequest(const PeerId& peer,
                                 proto::ChunkRequestMetadata& metadata);

 private:
  virtual void bulkInsertLocked(const MutableRevisionMap& items,
                                const LogicalTime& time) override {}
  virtual void updateLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {}
  virtual void removeLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {}

  inline void syncLatestCommitTime(const Revision& item);

  uint64_t raftInsertRequest(const Revision::ConstPtr& item);

  /**
   * ==========================================
   * Handlers for RPCs addressed to this Chunk.
   * ==========================================
   */
  friend class NetTable;

  inline void handleRaftConnectRequest(const PeerId& sender, Message* response);
  inline void handleRaftAppendRequest(proto::AppendEntriesRequest* request,
                                      const PeerId& sender, Message* response);
  inline void handleRaftInsertRequest(proto::InsertRequest* request,
                                      const PeerId& sender, Message* response);
  inline void handleRaftRequestVote(const proto::VoteRequest& request,
                                    const PeerId& sender, Message* response);
  inline void handleRaftQueryState(const proto::QueryState& request,
                                   Message* response);
  inline void handleRaftJoinQuitRequest(const proto::JoinQuitRequest& request,
                                        const PeerId& sender,
                                        Message* response);
  inline void handleRaftNotifyJoinQuitSuccess(
      const proto::NotifyJoinQuitSuccess& request, Message* response);

  virtual void leaveImpl() override {}
  virtual void awaitShared() override {}

 private:
  // Handles all communication with other chunk holders. No communication except
  // for peer join shall happen between chunk holder peers outside of raft.
  RaftNode raft_node_;
  volatile bool initialized_ = false;
  volatile bool relinquished_ = false;
  LogicalTime latest_commit_time_;
  uint64_t latest_commit_log_index_;
  std::unique_ptr<RaftChunkDataRamContainer> data_container2_;
};

#include "./raft-chunk-inl.h"

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_H_
