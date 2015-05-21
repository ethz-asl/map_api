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

  // ====================
  // Not implemented yet.
  // ====================
  virtual size_t numItems(const LogicalTime& time) const override;
  virtual size_t itemsSizeBytes(const LogicalTime& time) const override;
  virtual void getCommitTimes(const LogicalTime& sample_time,
                              std::set<LogicalTime>* commit_times) const override;
  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& item) override {
    LOG(WARNING) << "RaftChunk::insert() is not implemented";
    return true;
  }
  inline virtual int peerSize() const override;

  // Mutable because the method declarations in base class are const.
  mutable bool is_raft_write_locked_;
  mutable int write_lock_depth_;
  mutable std::mutex write_lock_mutex_;
  virtual void writeLock() override;
  virtual void readLock() const override {}  // No read lock for raft chunks.
  virtual bool isWriteLocked() override;
  virtual void unlock() const override;

  virtual int requestParticipation() override {
    LOG(WARNING) << "RaftChunk::requestParticipation() is not implemented";
    return 1;
  }
  virtual int requestParticipation(const PeerId& peer) override {
    LOG(WARNING) << "RaftChunk::requestParticipation() is not implemented";
    return 1;
  }
  virtual void update(const std::shared_ptr<Revision>& item) override {
    LOG(WARNING) << "RaftChunk::insert() is not implemented";
  }
  virtual LogicalTime getLatestCommitTime() const override {
    LOG(WARNING) << "RaftChunk::update() is not implemented";
    return LogicalTime::sample();
  }
  virtual void bulkInsertLocked(const MutableRevisionMap& items,
                                const LogicalTime& time) override {
    LOG(WARNING) << "RaftChunk::bulkInsertLocked() is not implemented";
  }
  virtual void updateLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {
    LOG(WARNING) << "RaftChunk::updateLocked() is not implemented";
  }
  virtual void removeLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {
    LOG(WARNING) << "RaftChunk::removeLocked() is not implemented";
  }
  virtual void leaveImpl() override {
    LOG(WARNING) << "RaftChunk::leaveImpl() is not implemented";
  }
  virtual void awaitShared() override {
    LOG(WARNING) << "RaftChunk::awaitShared() is not implemented";
  }
  // ========================================================================

  static bool sendConnectRequest(const PeerId& peer,
                                 proto::ChunkRequestMetadata& metadata);

 private:
  volatile bool initialized_ = false;
  volatile bool relinquished_ = false;

  // Handles all communication with other chunk holders. No communication except
  // for peer join shall happen between chunk holder peers outside of raft.
  RaftNode raft_node_;

  // TODO(aqurai): Replace arg with proto::Revision when implementing
  // transactions. Also add logical time.
  uint64_t insertRequest(const Revision::ConstPtr& item);

  /**
   * ==========================================
   * Handlers for RPCs addressed to this Chunk.
   * ==========================================
   */
  friend class NetTable;

  void handleRaftConnectRequest(const PeerId& sender, Message* response);
  void handleRaftAppendRequest(proto::AppendEntriesRequest* request,
                               const PeerId& sender, Message* response);
  void handleRaftInsertRequest(proto::InsertRequest* request,
                               const PeerId& sender, Message* response);
  void handleRaftRequestVote(const proto::VoteRequest& request,
                             const PeerId& sender, Message* response);
  void handleRaftQueryState(const proto::QueryState& request,
                            Message* response);
  void handleRaftJoinQuitRequest(const proto::JoinQuitRequest& request,
                                 const PeerId& sender, Message* response);
  void handleRaftNotifyJoinQuitSuccess(
      const proto::NotifyJoinQuitSuccess& request, Message* response);
};

}  // namespace map_api

#include "./raft-chunk-inl.h"

#endif  // MAP_API_RAFT_CHUNK_H_
