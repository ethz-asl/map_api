#ifndef READER_WRITER_MUTEX_FIXTURE_INL_H
#define READER_WRITER_MUTEX_FIXTURE_INL_H

#include <gtest/gtest.h>

#include "./reader_writer_mutex_fixture.h"

namespace map_api {

void ReaderWriterMutexFixture::SetUp() {
  ::testing::Test::SetUp();
  value_ = 0;
  num_writes_ = 0;
  num_upgrade_failures_ = 0;
}

void ReaderWriterMutexFixture::reader() {
  // CHECK_NOTNULL(value_mutex_);
  for (int i = 0; i < kNumCycles; ++i) {
    map_api::ScopedReadLock lock(&value_mutex_);
    EXPECT_EQ(0, value_ % kMagicNumber);
  }
}

void ReaderWriterMutexFixture::writer() {
  // CHECK_NOTNULL(value_mutex_);
  for (int i = 0; i < kNumCycles; ++i) {
    map_api::ScopedWriteLock lock(&value_mutex_);
    value_ = i * kMagicNumber;
  }
}

void ReaderWriterMutexFixture::delayedReader() {
  // CHECK_NOTNULL(value_mutex_);
  // CHECK_NOTNULL(num_writes_mutex_);
  for (int i = 0; i < kNumCycles; ++i) {
    value_mutex_.acquireReadLock();
    usleep(5);
    // num_writes_mutex_.acquireReadLock();
    EXPECT_EQ(value_, (num_writes_) * kMagicNumber);
    // num_writes_mutex_.releaseReadLock();
    value_mutex_.releaseReadLock();
  }
}

void ReaderWriterMutexFixture::readerUpgrade() {
  // CHECK_NOTNULL(value_mutex_);
  // CHECK_NOTNULL(num_writes_mutex_);
  for (int i = 0; i < kNumCycles; ++i) {
    value_mutex_.acquireReadLock();
    EXPECT_EQ(0, value_ % kMagicNumber);
    int read_value = value_;
    usleep(5);
    if (value_mutex_.upgradeToWriteLock()) {
      value_ = read_value + kMagicNumber;
      // num_writes_mutex_.acquireWriteLock();
      ++(num_writes_);
      // num_writes_mutex_.releaseWriteLock();
      value_mutex_.releaseWriteLock();
    } else {
      ++num_upgrade_failures_;
    }
  }
}

}  // namespace map_api

#endif  // READER_WRITER_MUTEX_FIXTURE_INL_H
