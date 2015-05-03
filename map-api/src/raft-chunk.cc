#include <map-api/raft-chunk.h>

#include <multiagent-mapping-common/conversions.h>

#include "./core.pb.h"
#include "./chunk.pb.h"
#include "map-api/raft-chunk-data-ram-container.h"
#include "map-api/legacy-chunk-data-ram-container.h"
#include "map-api/raft-node.h"
#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/net-table-manager.h"

namespace map_api {

RaftChunk::~RaftChunk() {
  raft_node_.stop();
}

bool RaftChunk::init(const common::Id& id,
                     std::shared_ptr<TableDescriptor> descriptor,
                     bool initialize) {
  id_ = id;
  data_container_.reset(new LegacyChunkDataRamContainer);
  CHECK(data_container_->init(descriptor));
  initialized_ = true;
  raft_node_.chunk_id_ = id_;
  raft_node_.table_name_ = descriptor->name();
  // TODO(aqurai) : This defeats the purpose of unique_ptr! Refactor:
  // Move data_container_ member of ChunkBase to le
  // raft_node_.data_container_ = data_container_.get();
  return true;
}

void RaftChunk::initializeNewImpl(
    const common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  CHECK(init(id, descriptor, true));

  VLOG(1) << " INIT chunk at peer " << PeerId::self() << " in table "
          << raft_node_.table_name_;
  setStateFollowerAndStartRaft();
}

bool RaftChunk::init(const common::Id& id,
                     std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(init(id, descriptor, true));
  setStateFollowerAndStartRaft();
  return true;
}

bool RaftChunk::init(const common::Id& id,
                     const proto::InitRequest& init_request,
                     std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(init(id, descriptor, true));

  VLOG(1) << " INIT chunk at peer " << PeerId::self() << " in table "
          << raft_node_.table_name_;


  raft_node_.initChunkData(init_request);
  setStateFollowerAndStartRaft();
  return true;
}

void RaftChunk::dumpItems(const LogicalTime& time, ConstRevisionMap* items) const {
  CHECK_NOTNULL(items);
  data_container_->dump(time, items);
}

size_t RaftChunk::numItems(const LogicalTime& time) const {
  return data_container_->numAvailableIds(time);
}

size_t RaftChunk::itemsSizeBytes(const LogicalTime& time) const {
  ConstRevisionMap items;
  data_container_->dump(time, &items);
  size_t num_bytes = 0;
  for (const std::pair<common::Id, std::shared_ptr<const Revision> >& item :
       items) {
    CHECK(item.second != nullptr);
    const Revision& revision = *item.second;
    num_bytes += revision.byteSize();
  }
  return num_bytes;
}

void RaftChunk::getCommitTimes(const LogicalTime& sample_time, std::set<LogicalTime>* commit_times) const {
  // TODO(aqurai): Implement this after data container implementation.
}

void RaftChunk::writeLock() {
  std::lock_guard<std::mutex> lock_mutex(write_lock_mutex_);
  if (is_raft_write_locked_) {
    ++write_lock_depth_;
  } else {
    // Send lock request via safe insert log entry.
    if (true /* Success */) {
      is_raft_write_locked_ = true;
    }
  }
}

bool RaftChunk::isWriteLocked() {
  std::lock_guard<std::mutex> lock(write_lock_mutex_);
  // return is_raft_write_locked_;
  return true;
}

void RaftChunk::unlock() const {
  std::lock_guard<std::mutex> lock(write_lock_mutex_);
  if (write_lock_depth_ > 0) {
    --write_lock_depth_;
  }
  if (write_lock_depth_ == 0) {
    // Send unlock request to leader.
    // There is no reason for this request to fail.
    CHECK(true /* unlock request success */);
    is_raft_write_locked_ = false;
  }
}

bool RaftChunk::sendConnectRequest(const PeerId& peer,
                                   proto::ChunkRequestMetadata& metadata) {
  Message request, response;
  proto::ConnectResponse connect_response;
  connect_response.set_index(0);
  request.impose<RaftNode::kConnectRequest>(metadata);

  // TODO(aqurai): Avoid infinite loop. Use Chord index to get chunk holder
  // if request fails.
  PeerId request_peer = peer;
  while (connect_response.index() == 0) {
    if (!(Hub::instance().try_request(request_peer, &request, &response))) {
      break;
    }
    response.extract<RaftNode::kConnectResponse>(&connect_response);
    if (connect_response.index() > 0) {
      return true;
    } else if (connect_response.has_leader_id()) {
      request_peer = PeerId(connect_response.leader_id());
    }
    usleep(1000);
  }
  return false;
}

uint64_t RaftChunk::insertRequest(const std::shared_ptr<Revision>& item) {
  CHECK(raft_node_.isRunning());
  return raft_node_.sendInsertRequest(item);
}

void RaftChunk::handleRaftConnectRequest(const PeerId& sender, Message* response) {
  raft_node_.handleConnectRequest(sender, response);
}

void RaftChunk::handleRaftAppendRequest(proto::AppendEntriesRequest* request,
                                        const PeerId& sender,
                                        Message* response) {
  raft_node_.handleAppendRequest(request, sender, response);
}

void RaftChunk::handleRaftInsertRequest(const proto::InsertRequest& request,
                                        const PeerId& sender,
                                        Message* response) {
  raft_node_.handleInsertRequest(request, sender, response);
}

void RaftChunk::handleRaftRequestVote(const proto::VoteRequest& request,
                                      const PeerId& sender, Message* response) {
  raft_node_.handleRequestVote(request, sender, response);
}

void RaftChunk::handleRaftQueryState(const proto::QueryState& request,
                                     Message* response) {
  raft_node_.handleQueryState(request, response);
}

void RaftChunk::handleRaftJoinQuitRequest(const proto::JoinQuitRequest& request,
                                          const PeerId& sender,
                                          Message* response) {
  raft_node_.handleJoinQuitRequest(request, sender, response);
}

void RaftChunk::handleRaftNotifyJoinQuitSuccess(
    const proto::NotifyJoinQuitSuccess& request, Message* response) {
  raft_node_.handleNotifyJoinQuitSuccess(request, response);
}

}  // namespace map_api
