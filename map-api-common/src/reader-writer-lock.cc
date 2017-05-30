// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#include "map-api-common/reader-writer-lock.h"

// Adapted from http://www.paulbridger.com/read_write_lock/
namespace map_api_common {

ReaderWriterMutex::ReaderWriterMutex()
    : num_readers_(0),
      num_pending_writers_(0),
      current_writer_(false),
      pending_upgrade_(false) {}

ReaderWriterMutex::~ReaderWriterMutex() {}

void ReaderWriterMutex::acquireReadLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (num_pending_writers_ != 0 || pending_upgrade_ || current_writer_) {
    m_writerFinished.wait(lock);
  }
  ++num_readers_;
}

void ReaderWriterMutex::releaseReadLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  --num_readers_;
  if (num_readers_ == (pending_upgrade_ ? 1 : 0)) {
    cv_readers.notify_all();
  }
}

void ReaderWriterMutex::acquireWriteLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  ++num_pending_writers_;
  while (num_readers_ > (pending_upgrade_ ? 1 : 0)) {
    cv_readers.wait(lock);
  }
  while (current_writer_ || pending_upgrade_) {
    m_writerFinished.wait(lock);
  }
  --num_pending_writers_;
  current_writer_ = true;
}

void ReaderWriterMutex::releaseWriteLock() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    current_writer_ = false;
  }
  m_writerFinished.notify_all();
}

// Attempt upgrade. If upgrade fails, relinquish read lock.
bool ReaderWriterMutex::upgradeToWriteLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pending_upgrade_) {
    --num_readers_;
    if (num_readers_ == 1) {
      cv_readers.notify_all();
    }
    return false;
  }
  pending_upgrade_ = true;
  while (num_readers_ > 1) {
    cv_readers.wait(lock);
  }
  pending_upgrade_ = false;
  --num_readers_;
  current_writer_ = true;
  if (num_readers_ == 0) {
    cv_readers.notify_all();
  }
  return true;
}

ScopedReadLock::ScopedReadLock(ReaderWriterMutex* rw_lock) : rw_lock_(rw_lock) {
  CHECK_NOTNULL(rw_lock_)->acquireReadLock();
}

ScopedReadLock::~ScopedReadLock() { rw_lock_->releaseReadLock(); }

ScopedWriteLock::ScopedWriteLock(ReaderWriterMutex* rw_lock)
    : rw_lock_(rw_lock) {
  CHECK_NOTNULL(rw_lock_)->acquireWriteLock();
}
ScopedWriteLock::~ScopedWriteLock() { rw_lock_->releaseWriteLock(); }

}  // namespace map_api_common
