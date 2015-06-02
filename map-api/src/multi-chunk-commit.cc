#include "map-api/multi-chunk-commit.h"

#include <future>

#include <multiagent-mapping-common/reader-writer-lock.h>

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
      notifications_enable_(false) {
  current_transaction_id_.setInvalid();
}

void MultiChunkTransaction::initMultiChunkTransaction(
    const proto::MultiChunkTransactionInfo multi_chunk_data, uint num_entries) {
  std::lock_guard<std::mutex> lock(mutex_);
  common::ScopedWriteLock data_lock(&data_mutex_);
  CHECK_GT(num_entries, 0);
  state_ = State::LOCKED;
  current_transaction_id_.deserialize(multi_chunk_data.transaction_id());
  CHECK(current_transaction_id_.isValid());
  num_commits_received_ = 0;
  num_revision_entries_ = num_entries;
  multi_chunk_data_ = &multi_chunk_data;
  CHECK_NOTNULL(multi_chunk_data_);
  other_chunk_status_.clear();
  other_chunk_leaders_.clear();
  CHECK_EQ(multi_chunk_data_->chunk_list_size(),
           multi_chunk_data_->leader_id_size());
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id id(multi_chunk_data_->chunk_list(i).chunk_id());
    if (id != my_chunk_id_) {
      other_chunk_status_.insert(std::make_pair(id, OtherChunkStatus::UNKNOWN));
      other_chunk_leaders_.insert(
          std::make_pair(id, PeerId(multi_chunk_data_->leader_id(i))));
    }
  }
}

void MultiChunkTransaction::clearMultiChunkTransaction() {
  std::lock_guard<std::mutex> lock(mutex_);
  common::ScopedWriteLock data_lock(&data_mutex_);
  state_ = State::INACTIVE;
  current_transaction_id_.setInvalid();
  num_commits_received_ = 0;
  num_revision_entries_ = 0;
  multi_chunk_data_ = NULL;
  other_chunk_status_.clear();
  other_chunk_leaders_.clear();
}

void MultiChunkTransaction::notifyReceivedRevisionIfActive() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (multi_chunk_data_ == NULL) {
    return;
  }
  CHECK(state_ != State::INACTIVE)
      << "Entry received notification when state is Inactive";
  if (state_ == State::LOCKED) {
    ++num_commits_received_;
  }
  if (num_commits_received_ == num_revision_entries_) {
    state_ = State::RECEIVED_ALL_ENTRIES;
  }
}

void MultiChunkTransaction::notifyUnlockAndCommitReceived() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == State::RECEIVED_ALL_ENTRIES) {
    setStateAwaitCommitLocked();
    lock.unlock();
  } else if (state_ != State::AWAIT_COMMIT || state_ == State::COMMITTED) {
    LOG(FATAL) << "Invalid transition from current state to AWAIT_COMMIT";
  }
}

void MultiChunkTransaction::notifyProceedCommit(NotificationMode mode) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == State::RECEIVED_ALL_ENTRIES) {
    setStateAwaitCommitLocked();
    lock.unlock();
    if (mode == NotificationMode::NOTIFY) {
      sendCommitNotification();
    }
  } else if (state_ != State::AWAIT_COMMIT || state_ == State::COMMITTED) {
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

void MultiChunkTransaction::notifyAbort(NotificationMode mode) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == State::LOCKED || state_ == State::RECEIVED_ALL_ENTRIES) {
    state_ = State::ABORTED;
    lock.unlock();
    if (mode == NotificationMode::NOTIFY) {
      sendAbortNotification();
    }
  } else if (state_ == State::AWAIT_COMMIT || state_ == State::COMMITTED) {
    LOG(FATAL) << "Invalid transition from current state to ABORTED";
  }
}

bool MultiChunkTransaction::isActive() {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_transaction_id_.isValid();
}

bool MultiChunkTransaction::isAborted() {
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(current_transaction_id_.isValid());
  return (state_ == State::ABORTED);
}

bool MultiChunkTransaction::isReadyToCommit() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == State::AWAIT_COMMIT) {
    return true;
  } else if (state_ == State::RECEIVED_ALL_ENTRIES) {
    if (areAllOtherChunksReadyToCommit(&lock)) {
      if (!(state_ == State::ABORTED)) {
        setStateAwaitCommitLocked();
        return true;
      }
    }
  }
  return false;
}

bool MultiChunkTransaction::areAllOtherChunksReadyToCommit(
    std::unique_lock<std::mutex>* lock) {
  CHECK(lock->owns_lock());
  std::unordered_set<common::Id> ready_chunks;
  typedef std::unordered_map<common::Id, OtherChunkStatus>::value_type
      ChunkStatus;
  for (ChunkStatus& chunk_status : other_chunk_status_) {
    if (chunk_status.second == OtherChunkStatus::READY) {
      ready_chunks.insert(chunk_status.first);
    }
  }
  lock->unlock();
  sendQueryReadyToCommit(ready_chunks);

  lock->lock();
  typedef std::unordered_map<common::Id, OtherChunkStatus>::value_type
      ChunkStatus;
  for (ChunkStatus& chunk_status : other_chunk_status_) {
    if (chunk_status.second != OtherChunkStatus::READY) {
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

void MultiChunkTransaction::sendQueryReadyToCommit(
    const std::unordered_set<common::Id>& ready_chunks) {
  std::map<common::Id, std::future<bool>> response_map;
  {
    common::ScopedReadLock lock(&data_mutex_);
    CHECK_NOTNULL(multi_chunk_data_);

    for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
      common::Id chunk_id(multi_chunk_data_->chunk_list(i).chunk_id());
      if (!ready_chunks.count(chunk_id)) {
        proto::MultiChunkTransactionQuery query;
        query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
        query.mutable_transaction_id()->CopyFrom(
            multi_chunk_data_->transaction_id());
        my_chunk_id_.serialize(query.mutable_sender_chunk_id());

        std::future<bool> result;
        std::async(std::launch::async,
                   &MultiChunkTransaction::sendMessage<kIsReadyToCommit>, this,
                   chunk_id, query);
        response_map.insert(std::make_pair(chunk_id, std::move(result)));
      }
    }
  }
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    response.second.wait();
  }
  std::lock_guard<std::mutex> lock(mutex_);
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    addOtherChunkStatusLocked(response.first, response.second.get());
  }
}

void MultiChunkTransaction::sendCommitNotification() {
  common::ScopedReadLock lock(&data_mutex_);
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id chunk_id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkTransactionQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_transaction_id()->CopyFrom(
        multi_chunk_data_->transaction_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
               &MultiChunkTransaction::sendMessage<kCommitNotification>, this,
               chunk_id, query);
    response_map.insert(std::make_pair(chunk_id, std::move(success)));
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
  common::ScopedReadLock lock(&data_mutex_);
  CHECK_NOTNULL(multi_chunk_data_);
  std::map<common::Id, std::future<bool>> response_map;
  for (int i = 0; i < multi_chunk_data_->chunk_list_size(); ++i) {
    common::Id chunk_id(multi_chunk_data_->chunk_list(i).chunk_id());
    proto::MultiChunkTransactionQuery query;
    query.mutable_metadata()->CopyFrom(multi_chunk_data_->chunk_list(i));
    query.mutable_transaction_id()->CopyFrom(
        multi_chunk_data_->transaction_id());
    my_chunk_id_.serialize(query.mutable_sender_chunk_id());

    std::future<bool> success;
    std::async(std::launch::async,
               &MultiChunkTransaction::sendMessage<kCommitNotification>, this,
               chunk_id, query);
    response_map.insert(std::make_pair(chunk_id, std::move(success)));
  }
  for (std::map<common::Id, std::future<bool>>::value_type& response :
       response_map) {
    response.second.wait();
  }
}

template <const char* message_type>
bool MultiChunkTransaction::sendMessage(
    const common::Id& chunk_id,
    const proto::MultiChunkTransactionQuery& query) {
  Message request, response;
  request.impose<message_type>(query);

  // TODO(aqurai): Use net table index to get chunk holder/leader. Issue #2466.
  std::unordered_map<common::Id, PeerId>::iterator found =
      other_chunk_leaders_.find(chunk_id);
  CHECK(found != other_chunk_leaders_.end());

  LOG(WARNING) << "Peer to which req is sent may not be leader now.";
  if (Hub::instance().try_request(found->second, &request, &response)) {
    return response.isOk();
  } else {
    return false;
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
    response->ack();
    return;
  } else if (current_transaction_id_.isValid() &&
             transaction_id == current_transaction_id_) {
    CHECK(state_ != State::INACTIVE);

    // If state is COMMITTED/AWAIT_COMMIT, the transaction would have been
    // already added to older_commits_, returning ack above.
    if (state_ == State::RECEIVED_ALL_ENTRIES) {
      common::Id sender_chunk_id(query.sender_chunk_id());
      addOtherChunkStatusLocked(sender_chunk_id, true);
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
  if (older_commits_.count(transaction_id)) {
    response->ack();
    return;
  } else if (current_transaction_id_.isValid() &&
             transaction_id == current_transaction_id_) {
    CHECK(state_ == State::RECEIVED_ALL_ENTRIES);
    setStateAwaitCommitLocked();
    response->ack();
  } else {
    LOG(FATAL) << "Another chunk is committing transaction while this chunk is "
                  "not ready";
  }
}

void MultiChunkTransaction::handleAbortNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  common::Id transaction_id(query.transaction_id());
  CHECK(transaction_id.isValid()) << "handleCommitNotification received from "
                                  << sender << "with an invalid transaction id";
  std::lock_guard<std::mutex> lock(mutex_);
  if (older_commits_.count(transaction_id)) {
    LOG(FATAL) << "Abort received after transaction committed on "
               << my_chunk_id_;
  }
  if (current_transaction_id_.isValid() &&
      transaction_id == current_transaction_id_) {
    CHECK(state_ != State::AWAIT_COMMIT || state_ != State::COMMITTED)
        << "Abort received after transaction committed on " << my_chunk_id_;
    state_ = State::ABORTED;
    response->ack();
  }
}

void MultiChunkTransaction::addOtherChunkStatusLocked(
    const common::Id& chunk_id, bool is_ready_to_commit) {
  if (other_chunk_status_.count(chunk_id) == 0) {
    LOG(FATAL) << "The specified Id is not present in the list.";
    return;
  }
  if (is_ready_to_commit) {
    other_chunk_status_[chunk_id] = OtherChunkStatus::READY;
  } else {
    other_chunk_status_[chunk_id] = OtherChunkStatus::NOT_READY;
  }
}

bool MultiChunkTransaction::isChunkReadyToCommitLocked(
    const common::Id& Chunk_id) {
  if (other_chunk_status_.count(Chunk_id) == 0) {
    LOG(FATAL) << "The specified Id is not present in the list.";
  }
  return (other_chunk_status_[Chunk_id] == OtherChunkStatus::READY);
}

}  // namespace map_api
