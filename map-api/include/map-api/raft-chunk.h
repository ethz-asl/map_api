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
  bool init(const common::Id& id, const proto::InitRequest& request,
            const PeerId& sender, std::shared_ptr<TableDescriptor> descriptor);
  virtual void dumpItems(const LogicalTime& time, ConstRevisionMap* items) const
      override;

  
  // -------------------------- Functions from the base class to be impl here.
  
  virtual size_t numItems(const LogicalTime& time) const override {return 0;};
  virtual size_t itemsSizeBytes(const LogicalTime& time) const override {return 0;};

  virtual void getCommitTimes(const LogicalTime& sample_time,
                              std::set<LogicalTime>* commit_times) const override {};

  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& item) override {return true; }

  virtual int peerSize() const override {return raft_node_.num_peers_;};

  // Non-const intended to avoid accidental write-lock while reading.
  virtual void writeLock() override {};
  // Doesn't need to be implemented if race conditions with committing can be
  // avoided otherwise.
  virtual void readLock() const override {};

  virtual bool isWriteLocked() override {return true;};

  virtual void unlock() const override {};
  
  virtual int requestParticipation() override {return 1;}
  virtual int requestParticipation(const PeerId& peer) override {return 1;}
  virtual void update(const std::shared_ptr<Revision>& item) override {}
  virtual LogicalTime getLatestCommitTime() const override {return LogicalTime::sample();}
  virtual void bulkInsertLocked(const MutableRevisionMap& items,
                                const LogicalTime& time) override {}
  virtual void updateLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {}
  virtual void removeLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override {}
  virtual void leaveImpl() override {}
  virtual void awaitShared() override {}
  
  // --------------------------------------------------------------------
  
  
 private:
  volatile bool initialized_ = false;
  volatile bool relinquished_ = false;
  
  // Handles all communication with other chunk holders. No communication except
  // for peer join shall happen between chunk holder peers outside of raft.
  RaftNode raft_node_;
  
  template <typename RequestType>
  void fillMetadata(RequestType* destination) const;
  
  /**
   * ==========================================
   * Handlers for RPCs addressed to this Chunk.
   * ==========================================
   */
  friend class NetTable;
  
  // TODO(aqurai): Pass only relevant objects as arguments.
  void handleRaftAppendRequest(const common::Id& chunk_id, const Message& request, Message* response);
  void handleRaftRequestVote(const common::Id& chunk_id, const Message& request, Message* response);
  void handleRaftQueryState(const common::Id& chunk_id, const Message& request, Message* response);
  void handleRaftJoinQuitRequest(const common::Id& chunk_id, const Message& request, Message* response);
  void handleRaftNotifyJoinQuitSuccess(const common::Id& chunk_id, const Message& request, Message* response);
  
  
  
};

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_H_
