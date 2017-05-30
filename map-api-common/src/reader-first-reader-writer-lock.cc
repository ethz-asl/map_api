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

#include "map-api-common/reader-first-reader-writer-lock.h"

namespace map_api_common {

ReaderFirstReaderWriterMutex::ReaderFirstReaderWriterMutex()
    : ReaderWriterMutex() {}

ReaderFirstReaderWriterMutex::~ReaderFirstReaderWriterMutex() {}

void ReaderFirstReaderWriterMutex::acquireReadLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (current_writer_) {
    m_writerFinished.wait(lock);
  }
  ++num_readers_;
}

void ReaderFirstReaderWriterMutex::acquireWriteLock() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    while (num_readers_ > (pending_upgrade_ ? 1 : 0)) {
      cv_readers.wait(lock);
    }
    if (!current_writer_ && !pending_upgrade_) {
      break;
    } else {
      while (current_writer_ || pending_upgrade_) {
        m_writerFinished.wait(lock);
      }
    }
  }
  current_writer_ = true;
}

}  // namespace map_api_common
