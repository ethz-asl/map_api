#include <map-api/raft-chunk.h>

#include <multiagent-mapping-common/conversions.h>

#include "./core.pb.h"
#include "./chunk.pb.h"
#include "map-api/chunk-data-ram-container.h"
#include "map-api/raft-node.h"
#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/net-table-manager.h"

namespace map_api {

RaftChunk::~RaftChunk() {}

bool RaftChunk::init(const common::Id& id, std::shared_ptr<TableDescriptor> descriptor,
            bool initialize) {
  id_ = id;
  // TODO(aqurai): init data container.
  data_container_.reset(new ChunkDataRamContainer);
  CHECK(data_container_->init(descriptor));
  initialized_ = true;
  raft_node_.chunk_id_ = id_;
  raft_node_.table_name_ = descriptor->name();
  return true;
}

void RaftChunk::initializeNewImpl(
    const common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  CHECK(init(id, descriptor, true));

  VLOG(1) << " INIT chunk at peer " << PeerId::self() << " in table "
          << raft_node_.table_name_;
  setStateFollowerAndStart();
}

bool RaftChunk::init(const common::Id& id, const PeerId& peer,
                     std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(init(id, descriptor, true));
  setStateJoiningAndStart(peer);
}

bool RaftChunk::init(const common::Id& id,
                     std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(init(id, descriptor, true));
  setStateFollowerAndStart();
  return true;
}

void RaftChunk::dumpItems(const LogicalTime& time, ConstRevisionMap* items) const {
  CHECK_NOTNULL(items);
  data_container_->dump(time, items);
}

bool RaftChunk::sendConnectRequest(const PeerId& peer,
                                   proto::ChunkRequestMetadata& metadata) {
  Message request, response;
  proto::JoinQuitRequest connect_request;
  proto::JoinQuitResponse connect_response;
  connect_request.set_allocated_metadata(&metadata);
  request.impose<RaftNode::kJoinQuitRequest>(connect_request);

  connect_response.set_response(false);
  // TODO(aqurai): Avoid infinite loop.
  while (!connect_response.response()) {
    Hub::instance().try_request(peer, &request, &response);
    response.extract<RaftNode::kJoinQuitResponse>(&connect_response);
    if (!connect_response.response()) {
      if (connect_response.has_leader_id()) {
        Hub::instance().try_request(PeerId(connect_response.leader_id()),
                                    &request, &response);
        response.extract<RaftNode::kJoinQuitResponse>(&connect_response);
      }
    }
    usleep(1000 * kMillisecondsToMicroseconds);
  }
  connect_request.release_metadata();
  return true;
}

void RaftChunk::handleConnectRequest(const Message& request,
                                     Message* response) {}

void RaftChunk::handleRaftAppendRequest(proto::AppendEntriesRequest& request,
                                        const PeerId& sender,
                                        Message* response) {
  raft_node_.handleAppendRequest(request, sender, response);
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
