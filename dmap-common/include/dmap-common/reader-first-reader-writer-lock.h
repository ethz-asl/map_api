#ifndef DMAP_COMMON_READER_FIRST_READER_WRITER_LOCK_H_
#define DMAP_COMMON_READER_FIRST_READER_WRITER_LOCK_H_

#include "dmap-common/reader-writer-lock.h"

namespace dmap_common {

class ReaderFirstReaderWriterMutex : public ReaderWriterMutex {
 public:
  ReaderFirstReaderWriterMutex();
  ~ReaderFirstReaderWriterMutex();

  virtual void acquireReadLock() override;

  virtual void acquireWriteLock() override;
};

}  // namespace dmap_common

#endif  // DMAP_COMMON_READER_FIRST_READER_WRITER_LOCK_H_
