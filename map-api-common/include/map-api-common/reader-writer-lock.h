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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_COMMON_READER_WRITER_LOCK_H_
#define MAP_API_COMMON_READER_WRITER_LOCK_H_

#include <condition_variable>
#include <mutex>

#include <glog/logging.h>

// Adapted from http://www.paulbridger.com/read_write_lock/
namespace map_api_common {
class ReaderWriterMutex {
 public:
  ReaderWriterMutex();
  virtual ~ReaderWriterMutex();

  virtual void acquireReadLock();
  void releaseReadLock();

  virtual void acquireWriteLock();
  virtual void releaseWriteLock();

  // Attempt upgrade. If upgrade fails, relinquish read lock.
  virtual bool upgradeToWriteLock();

 protected:
  ReaderWriterMutex(const ReaderWriterMutex&) = delete;
  ReaderWriterMutex& operator=(const ReaderWriterMutex&) = delete;

  std::mutex mutex_;
  unsigned int num_readers_;
  std::condition_variable cv_readers;
  unsigned int num_pending_writers_;
  bool current_writer_;
  std::condition_variable m_writerFinished;
  bool pending_upgrade_;
};

class ScopedReadLock {
 public:
  explicit ScopedReadLock(ReaderWriterMutex* rw_lock);
  ~ScopedReadLock();

 private:
  ScopedReadLock(const ScopedReadLock&) = delete;
  ScopedReadLock& operator=(const ScopedReadLock&) = delete;
  ReaderWriterMutex* rw_lock_;
};

class ScopedWriteLock {
 public:
  explicit ScopedWriteLock(ReaderWriterMutex* rw_lock);
  ~ScopedWriteLock();

 private:
  ScopedWriteLock(const ScopedWriteLock&) = delete;
  ScopedWriteLock& operator=(const ScopedWriteLock&) = delete;
  ReaderWriterMutex* rw_lock_;
};

}  // namespace map_api_common
#endif  // MAP_API_COMMON_READER_WRITER_LOCK_H_
