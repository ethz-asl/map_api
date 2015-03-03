#include <functional>
#include <memory>
#include <vector>
#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/test/testing-entrypoint.h"
#include "./reader_writer_mutex_fixture.h"

constexpr int kNumThreads = 20;

namespace map_api {

TEST_F(ReaderWriterMutexFixture, ReaderWriterLock) {
  map_api::ReaderWriterMutex mutex;
  std::vector < std::thread > threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this]() { reader(); });
    threads.emplace_back([this]() { writer(); });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(0, value() % kMagicNumber);
}
void delayedReader(int* value, int* num_writes,
                   map_api::ReaderWriterMutex* value_mutex,
                   map_api::ReaderWriterMutex* num_writes_mutex) {
  CHECK_NOTNULL(value);
  CHECK_NOTNULL(value_mutex);
  CHECK_NOTNULL(num_writes);
  CHECK_NOTNULL(num_writes_mutex);
  for (int i = 0; i < 1000; ++i) {
    value_mutex->acquireReadLock();
    usleep(5);
    num_writes_mutex->acquireReadLock();
    EXPECT_EQ(*value, (*num_writes) * kMagicNumber);
    num_writes_mutex->releaseReadLock();
    value_mutex->releaseReadLock();
  }
}
void readerUpgrade(int* value, int* num_writes,
                   map_api::ReaderWriterMutex* value_mutex,
                   map_api::ReaderWriterMutex* num_writes_mutex) {
  CHECK_NOTNULL(value);
  CHECK_NOTNULL(value_mutex);
  CHECK_NOTNULL(num_writes);
  CHECK_NOTNULL(num_writes_mutex);
  for (int i = 0; i < 1000; ++i) {
    value_mutex->acquireReadLock();
    EXPECT_EQ(0, *value % kMagicNumber);
    int read_value = *value;
    usleep(5);
    if (value_mutex->upgradeToWriteLock()) {
      *value = read_value + kMagicNumber;
      num_writes_mutex->acquireWriteLock();
      ++(*num_writes);
      num_writes_mutex->releaseWriteLock();
      value_mutex->releaseWriteLock();
    }
  }
}

TEST_F(ReaderWriterMutexFixture, UpgradeReaderLock) {
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this]() { delayedReader(); });
    threads.emplace_back([this]() { readerUpgrade(); });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  VLOG(3) << "Number of writes after upgrade: " << num_writes();
  VLOG(3) << "Number of failed upgrades: " << num_upgrade_failures();
  EXPECT_NE(0, value());
  EXPECT_NE(0, num_writes());
  EXPECT_EQ(value(), num_writes() * kMagicNumber);
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
