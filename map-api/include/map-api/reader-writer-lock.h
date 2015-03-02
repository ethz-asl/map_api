#ifndef MAP_API_READER_WRITER_LOCK_H_
#define MAP_API_READER_WRITER_LOCK_H_

#include <condition_variable>
#include <mutex>

#include <glog/logging.h>

// Adapted from http://www.paulbridger.com/read_write_lock/
namespace map_api {
class ReaderWriterMutex {
 public:
  friend class ScopedReadLock;
  friend class ScopedWriteLock;
  ReaderWriterMutex()
      : num_readers_(0),
        num_pending_writers_(0),
        current_writer_(false),
        num_pending_upgrade_(0) {}

 private:
  ReaderWriterMutex(const ReaderWriterMutex&) = delete;
  ReaderWriterMutex& operator=(const ReaderWriterMutex&) = delete;

  std::mutex mutex_;
  unsigned int num_readers_;
  std::condition_variable cv_readers;
  unsigned int num_pending_writers_;
  bool current_writer_;
  std::condition_variable m_writerFinished;
  unsigned int num_pending_upgrade_;

 public:
  void acquireReadLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_pending_writers_ != 0 || num_pending_upgrade_ != 0 ||
           current_writer_) {
      m_writerFinished.wait(lock);
    }
    ++num_readers_;
  }
  void releaseReadLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    --num_readers_;
    if (num_readers_ == num_pending_upgrade_) {
      cv_readers.notify_all();
    }
  }
  void acquireWriteLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++num_pending_writers_;
    while (num_readers_ > num_pending_upgrade_) {
      cv_readers.wait(lock);
    }
    while (current_writer_ || num_pending_upgrade_ != 0) {
      m_writerFinished.wait(lock);
    }
    --num_pending_writers_;
    current_writer_ = true;
  }
  void releaseWriteLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    current_writer_ = false;
    m_writerFinished.notify_all();
  }
  void upgradeToWriteLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++num_pending_upgrade_;
    while (num_readers_ > num_pending_upgrade_) {
      cv_readers.wait(lock);
    }
    --num_pending_upgrade_;
    --num_readers_;
    current_writer_ = true;
  }
};

class ScopedReadLock {
 public:
  explicit ScopedReadLock(ReaderWriterMutex* rw_lock)
      : rw_lock_(rw_lock) {
    CHECK_NOTNULL(rw_lock_)->acquireReadLock();
  }
  ~ScopedReadLock() {
    rw_lock_->releaseReadLock();
  }
 private:
  ScopedReadLock(const ScopedReadLock&) = delete;
  ScopedReadLock& operator=(const ScopedReadLock&) = delete;
  ReaderWriterMutex* rw_lock_;
};

class ScopedWriteLock {
 public:
  explicit ScopedWriteLock(ReaderWriterMutex* rw_lock)
      : rw_lock_(rw_lock) {
    CHECK_NOTNULL(rw_lock_)->acquireWriteLock();
  }
  ~ScopedWriteLock() {
    rw_lock_->releaseWriteLock();
  }
 private:
  ScopedWriteLock(const ScopedWriteLock&) = delete;
  ScopedWriteLock& operator=(const ScopedWriteLock&) = delete;
  ReaderWriterMutex* rw_lock_;
};

}  // namespace map_api
#endif  // MAP_API_READER_WRITER_LOCK_H_
