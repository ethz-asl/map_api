#include <functional>
#include <memory>

#include "map-api/reader-writer-lock.h"
#include "map-api/test/testing-entrypoint.h"
#include <iostream>

const int logd = 0;

int kMagicNumber = 29845;

void reader(int* n, map_api::ReaderWriterMutex* mutex) {
  CHECK_NOTNULL(n);
  CHECK_NOTNULL(mutex);
  for (int i = 0; i < 1000; ++i) {
    map_api::ScopedReadLock lock(mutex);
    EXPECT_EQ(0, *n % kMagicNumber);
  }
}
void writer(int* n, map_api::ReaderWriterMutex* mutex) {
  CHECK_NOTNULL(n);
  CHECK_NOTNULL(mutex);
  for (int i = 0; i < 1000; ++i) {
    map_api::ScopedWriteLock lock(mutex);
    *n = i * kMagicNumber;
  }
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

TEST(MultiagentMappingCommon, ReaderWriterLock) {
  int n = 0;
  map_api::ReaderWriterMutex mutex;
  std::vector < std::thread > threads;
  for (int i = 0; i < 20; ++i) {
    threads.emplace_back(reader, &n, &mutex);
    threads.emplace_back(writer, &n, &mutex);
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(0, n % kMagicNumber);
}

TEST(MultiagentMappingCommon, UpgradeReaderLock) {
  int value = 0;
  int num_writes = 0;
  map_api::ReaderWriterMutex num_writes_mutex;
  map_api::ReaderWriterMutex value_mutex;
  std::vector<std::thread> threads;
  for (int i = 0; i < 20; ++i) {
    threads.emplace_back(readerUpgrade, &value, &num_writes, &value_mutex,
                         &num_writes_mutex);
    threads.emplace_back(delayedReader, &value, &num_writes, &value_mutex,
                         &num_writes_mutex);
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_NE(0, value);
  EXPECT_NE(0, num_writes);
  EXPECT_EQ(value, num_writes * kMagicNumber);
}

MAP_API_UNITTEST_ENTRYPOINT
