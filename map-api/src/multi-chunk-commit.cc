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

const char MultiChunkTransaction::kIsReadyToCommit[] =
    "multi_chunk_transaction_is_ready";
const char MultiChunkTransaction::kCommitNotification[] =
    "multi_chunk_transaction_commit";
const char MultiChunkTransaction::kAbortNotification[] =
    "multi_chunk_transaction_abort";

MAP_API_PROTO_MESSAGE(MultiChunkTransaction::kIsReadyToCommit,
                      proto::MultiChunkTransactionQuery);
MAP_API_PROTO_MESSAGE(MultiChunkTransaction::kCommitNotification,
                      proto::MultiChunkTransactionQuery);
MAP_API_PROTO_MESSAGE(MultiChunkTransaction::kAbortNotification,
                      proto::MultiChunkTransactionQuery);

MultiChunkTransaction::MultiChunkTransaction(const common::Id& id)
    : my_chunk_id_(id),
      state_(State::INACTIVE),
      num_commits_received_(0),
      num_revision_entries_(0),
      asked_all_(false),
      notifications_enable_(false) {
  current_transaction_id_.setInvalid();
}

void MultiChunkTransaction::initMultiChunkTransaction(
    const proto::MultiChunkTransactionInfo multi_chunk_data, uint num_entries) {
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK_GT(num_entries, 0);
  state_ = State::LOCKED;
  current_transaction_id_.deserialize(multi_chunk_data.transaction_id());
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

void MultiChunkTransaction::clearMultiChunkTransaction() {
  std::lock_guard<std::mutex> lock(mutex_);
  state_ = State::INACTIVE;
  current_transaction_id_.setInvalid();
  num_commits_received_ = 0;
  num_revision_entries_ = 0;
  multi_chunk_data_ = NULL;
  other_chunk_status_.clear();
  asked_all_ = false;
}

void MultiChunkTransaction::notifyReceivedRevisionIfActive() {
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

void MultiChunkTransaction::noitfyCommitBegin() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::READY_TO_COMMIT) {
    state_ = State::AWAIT_COMMIT;
    sendCommitNotification();
  } else {
    LOG(FATAL) << "Invalid transition from current state to AWAIT_COMMIT";
  }
}

void MultiChunkTransaction::notifyCommitSuccess() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::AWAIT_COMMIT) {
    state_ = State::COMMITTED;
  } else {
    LOG(FATAL) << "Invalid transition from current state to COMMITTED";
  }
}

void MultiChunkTransaction::notifyAbort() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ == State::LOCKED || state_ == State::READY_TO_COMMIT) {
    state_ = State::ABORTED;
    sendAbortNotification();
  } else {
    LOG(FATAL) << "Invalid transition from current state to ABORTED";
  }
}

bool MultiChunkTransaction::isActive() {
  std::lock_guard<std::mutex> lock(mutex_);
  return (state_ == State::LOCKED || state_ == State::READY_TO_COMMIT);
}

bool MultiChunkTransaction::isReadyToCommit() {
  std::lock_guard<std::mutex> lock(mutex_);
  return (state_ == State::READY_TO_COMMIT || state_ == State::AWAIT_COMMIT);
}

bool MultiChunkTransaction::areAllOtherChunksReadyToCommit() {
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

bool MultiChunkTransaction::isTransactionCommitted(
    const common::Id& commit_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  return older_commits_.count(commit_id);
}

void MultiChunkTransaction::sendQueryReadyToCommit() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkTransactionQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_transaction_id()->CopyFrom(
        multi_chunk_data_->transaction_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
               &MultiChunkTransaction::sendMessage<kIsReadyToCommit>, this, id,
               query);
    response_map.insert(std::make_pair(id, std::move(success)));
  }

  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    addOtherChunkStatusLocked(response.first, response.second.get());
  }
}

void MultiChunkTransaction::sendCommitNotification() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkTransactionQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_transaction_id()->CopyFrom(
        multi_chunk_data_->transaction_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
               &MultiChunkTransaction::sendMessage<kCommitNotification>, this,
               id, query);
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

void MultiChunkTransaction::sendAbortNotification() {
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkTransactionQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_transaction_id()->CopyFrom(
        multi_chunk_data_->transaction_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
               &MultiChunkTransaction::sendMessage<kCommitNotification>, this,
               id, query);
    response_map.insert(std::make_pair(id, std::move(success)));
  }
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    response.second.wait();
  }
}

void MultiChunkTransaction::handleQueryReadyToCommit(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.transaction_id());
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

void MultiChunkTransaction::handleCommitNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.transaction_id());
  CHECK(transaction_id.isValid()) << "handleCommitNotification received from "
                                  << sender << "with an invalid transaction id";
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_transaction_id_.isValid() &&
      transaction_id == current_transaction_id_) {
    // TODO(aqurai): handle this
  }
}

void MultiChunkTransaction::handleAbortNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.transaction_id());
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
bool MultiChunkTransaction::sendMessage(
    const common::Id& id, const proto::MultiChunkTransactionQuery& query) {
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

void MultiChunkTransaction::fetchOtherChunkStatusLocked() {
  sendQueryReadyToCommit();
  asked_all_ = true;
}

void MultiChunkTransaction::addOtherChunkStatusLocked(const common::Id& id,
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
