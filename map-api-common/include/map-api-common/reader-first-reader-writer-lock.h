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

#ifndef MAP_API_COMMON_READER_FIRST_READER_WRITER_LOCK_H_
#define MAP_API_COMMON_READER_FIRST_READER_WRITER_LOCK_H_

#include "map-api-common/reader-writer-lock.h"

namespace map_api_common {

class ReaderFirstReaderWriterMutex : public ReaderWriterMutex {
 public:
  ReaderFirstReaderWriterMutex();
  ~ReaderFirstReaderWriterMutex();

  virtual void acquireReadLock() override;

  virtual void acquireWriteLock() override;
};

}  // namespace map_api_common

#endif  // MAP_API_COMMON_READER_FIRST_READER_WRITER_LOCK_H_
