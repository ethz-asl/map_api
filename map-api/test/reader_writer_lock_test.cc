#include <functional>
#include <memory>

#include "map-api/reader-writer-lock.h"
#include "map-api/test/testing-entrypoint.h"

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

MAP_API_UNITTEST_ENTRYPOINT
