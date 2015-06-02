#include "map-api/multi-chunk-commit.h"

#include <future>
#include <readline/history.h>

#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/net-table.h"
#include "map-api/net-table-manager.h"
#include "map-api/peer-id.h"

namespace map_api {

const char MultiChunkCommit::kIsReadyToCommit[] = "multi_chunk_commit_is_ready";
// const char MultiChunkCommit::kIsReadyResponse[] =
// "multi_chunk_commit_is_ready_response";
const char MultiChunkCommit::kCommitNotification[] =
    "multi_chunk_commit_commit";
const char MultiChunkCommit::kAbortNotification[] = "multi_chunk_commit_abort";

MAP_API_PROTO_MESSAGE(MultiChunkCommit::kIsReadyToCommit,
                      proto::MultiChunkCommitQuery);
MAP_API_PROTO_MESSAGE(MultiChunkCommit::kCommitNotification,
                      proto::MultiChunkCommitQuery);
MAP_API_PROTO_MESSAGE(MultiChunkCommit::kAbortNotification,
                      proto::MultiChunkCommitQuery);

MultiChunkCommit::MultiChunkCommit(const common::Id& id)
    : my_chunk_id_(id),
      state_(State::INACTIVE),
      num_commits_received_(0),
      num_revision_entries_(0),
      asked_all_(false),
      notifications_enable_(false) {
  current_transaction_id_.setInvalid();
}

void MultiChunkCommit::initMultiChunkCommit(
    const proto::MultiChunkCommitInfo multi_chunk_data, uint num_entries) {
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK_GT(num_entries, 0);
  state_ = State::LOCKED;
  current_transaction_id_.deserialize(multi_chunk_data.commit_id());
  CHECK(current_transaction_id_.isValid());
  num_commits_received_ = 0;
  num_revision_entries_ = num_entries;
  multi_chunk_data_ = &multi_chunk_data;
  CHECK_NOTNULL(multi_chunk_data_);
  other_chunk_status_.clear();
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    if (id != my_chunk_id_) {
      other_chunk_status_.insert(std::make_pair(id, OtherChunkStatus::UNKNOWN));
    }
  }
  asked_all_ = false;
}

void MultiChunkCommit::clearMultiChunkCommit() {
  std::lock_guard<std::mutex> lock(mutex_);
  state_ = State::INACTIVE;
  current_transaction_id_.setInvalid();
  num_commits_received_ = 0;
  num_revision_entries_ = 0;
  multi_chunk_data_ = NULL;
  other_chunk_status_.clear();
  asked_all_ = false;
}

void MultiChunkCommit::notifyReceivedRevisionIfActive() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (multi_chunk_data_ == NULL) {
    return;
  }
  CHECK(state_ == State::LOCKED)
      << "Entry received notification when state is not LOCKED";
  if (state_ == State::LOCKED) {
    ++num_commits_received_;
  }
  if (num_commits_received_ == num_revision_entries_) {
    state_ = State::READY_TO_COMMIT;
  }
}

void MultiChunkCommit::noitfyCommitBegin() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::READY_TO_COMMIT) {
    state_ = State::AWAIT_COMMIT;
    sendCommitNotification();
  } else {
    LOG(FATAL) << "Invalid transition from current state to AWAIT_COMMIT";
  }
}

void MultiChunkCommit::notifyCommitSuccess() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::AWAIT_COMMIT) {
    state_ = State::COMMITTED;
  } else {
    LOG(FATAL) << "Invalid transition from current state to COMMITTED";
  }
}

void MultiChunkCommit::notifyAbort() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::LOCKED || state_ == State::READY_TO_COMMIT) {
    state_ = State::ABORTED;
    sendAbortNotification();
  } else {
    LOG(FATAL) << "Invalid transition from current state to ABORTED";
  }
}

bool MultiChunkCommit::isActive() {
  std::lock_guard<std::mutex> lock(mutex_);
  return (state_ == State::LOCKED || state_ == State::READY_TO_COMMIT);
}

bool MultiChunkCommit::isReadyToCommit() {
  std::lock_guard<std::mutex> lock(mutex_);
  return (state_ == State::READY_TO_COMMIT || state_ == State::AWAIT_COMMIT);
}

bool MultiChunkCommit::areAllOtherChunksReadyToCommit() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!asked_all_) {
    fetchOtherChunkStatusLocked();
  }
  typedef std::unordered_map<common::Id, OtherChunkStatus>::value_type
      ChunkStatus;
  for (ChunkStatus& chunk_status : other_chunk_status_) {
    if (chunk_status.second == OtherChunkStatus::UNKNOWN) {
      LOG(ERROR) << "One of the ask-other-chunk messages failed!";
      return false;
    }
    if (chunk_status.second == OtherChunkStatus::NOT_READY) {
      return false;
    }
  }
  return true;
}

bool MultiChunkCommit::isTransactionCommitted(const common::Id& commit_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  return older_commits_.count(commit_id);
}

void MultiChunkCommit::sendQueryReadyToCommit() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkCommitQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_commit_id()->CopyFrom(multi_chunk_data_->commit_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
        &MultiChunkCommit::sendMessage<kIsReadyToCommit>, this, id, query);
    response_map.insert(std::make_pair(id, std::move(success)));
  }

  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    addOtherChunkStatusLocked(response.first, response.second.get());
  }
}

void MultiChunkCommit::sendCommitNotification() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkCommitQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_commit_id()->CopyFrom(multi_chunk_data_->commit_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
        &MultiChunkCommit::sendMessage<kCommitNotification>, this, id, query);
    response_map.insert(std::make_pair(id, std::move(success)));
  }
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    if (!response.second.get()) {
      LOG(ERROR) << "Multi-chunk transaction committed on " << my_chunk_id_
                 << " but notification failed for chunk " << response.first;
    }
  }
  // TODO(aqurai): We are not checking if one of the chunks fails here. We are
  // ensuring that a correct version of related entry in another chunk is
  // available or the entry is not available altogether, but a wrong version is
  // not never returned. Implement rollback here?
}

void MultiChunkCommit::sendAbortNotification() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkCommitQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_commit_id()->CopyFrom(multi_chunk_data_->commit_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
        &MultiChunkCommit::sendMessage<kCommitNotification>, this, id, query);
    response_map.insert(std::make_pair(id, std::move(success)));
  }
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    response.second.wait();
  }
}

void MultiChunkCommit::handleQueryReadyToCommit(
    const proto::MultiChunkCommitQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.commit_id());
  CHECK(transaction_id.isValid()) << "handleCommitNotification received from "
                                  << sender << "with an invalid transaction id";
  std::lock_guard<std::mutex> lock(mutex_);
  if (older_commits_.count(transaction_id)) {
    // This transaction has already been committed.
    response->ack();
    return;
  } else if (current_transaction_id_.isValid() &&
             transaction_id == current_transaction_id_) {
    CHECK(state_ != State::INACTIVE);
    // If state is COMMITTED, the transaction would have been already  added to
    // older_commits_, returning ack above.
    if (state_ == State::READY_TO_COMMIT || state_ == State::AWAIT_COMMIT) {
      response->ack();
      return;
    }
  } else {
    LOG(ERROR) << "QueryReadyToCommit received for a transaction id that is "
                  "neither current nor an older transaction. Sender: " << sender
               << ", chunk id: " << my_chunk_id_;
  }
  response->decline();
}

void MultiChunkCommit::handleCommitNotification(
    const proto::MultiChunkCommitQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.commit_id());
  CHECK(transaction_id.isValid()) << "handleCommitNotification received from "
                                  << sender << "with an invalid transaction id";
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_transaction_id_.isValid() &&
      transaction_id == current_transaction_id_) {
    // TODO(aqurai): handle this
  }
}

void MultiChunkCommit::handleAbortNotification(
    const proto::MultiChunkCommitQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.commit_id());
  CHECK(transaction_id.isValid()) << "handleCommitNotification received from "
                                  << sender << "with an invalid transaction id";
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_transaction_id_.isValid() &&
      transaction_id == current_transaction_id_) {
    CHECK(state_ != State::AWAIT_COMMIT || state_ != State::COMMITTED)
        << "Abort received after transaction committed on " << my_chunk_id_;
    state_ = State::ABORTED;
  }
}

template <const char* message_type>
bool MultiChunkCommit::sendMessage(const common::Id& id,
                                   const proto::MultiChunkCommitQuery& query) {
  Message request, response;
  request.impose<message_type>(query);

  // std::unordered_set<PeerId> peers;
  // NetTableManager.instance().getTable(query.metadata().table()).get
  // getChunkHolders(id, &peers);
  const PeerId peer;
  CHECK(false);
  // TODO(aqurai): get leader somehow. The peer should be leader at the time of
  // answering.
  LOG(WARNING) << "Peer to which req is sent may not be leader";
  if (Hub::instance().try_request(peer, &request, &response)) {
    return response.isOk();
  } else {
    return false;
  }
}

void MultiChunkCommit::fetchOtherChunkStatusLocked() {
  sendQueryReadyToCommit();
  asked_all_ = true;
}

void MultiChunkCommit::addOtherChunkStatusLocked(const common::Id& id,
                                                 bool is_ready_to_commit) {
  if (other_chunk_status_.count(id) == 0) {
    LOG(FATAL) << "The specified Id is not present in the list.";
    return;
  }
  if (is_ready_to_commit) {
    other_chunk_status_[id] = OtherChunkStatus::READY;
  } else {
    other_chunk_status_[id] = OtherChunkStatus::NOT_READY;
  }
}

}  // namespace map_api
