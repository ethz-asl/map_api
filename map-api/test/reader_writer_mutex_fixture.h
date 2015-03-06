#ifndef READER_WRITER_MUTEX_FIXTURE_H_
#define READER_WRITER_MUTEX_FIXTURE_H_

#include <string>
#include <atomic>

#include <gtest/gtest.h>

#include "map-api/reader-writer-lock.h"

constexpr int kMagicNumber = 29845;
constexpr int kNumCycles = 1000;

namespace map_api {

class ReaderWriterMutexFixture : public ::testing::Test {
 private:
  int value_;
  std::atomic<int> num_writes_;
  std::atomic<int> num_upgrade_failures_;

 protected:
  virtual void SetUp();

  void reader();
  void writer();
  void delayedReader();
  void readerUpgrade();

  int value() { return value_; }
  int num_writes() { return num_writes_; }
  int num_upgrade_failures() { return num_upgrade_failures_; }

  map_api::ReaderWriterMutex value_mutex_;
};

}  // namespace map_api

#include "./reader_writer_mutex_fixture_inl.h"

#endif  // READER_WRITER_MUTEX_FIXTURE_H_
