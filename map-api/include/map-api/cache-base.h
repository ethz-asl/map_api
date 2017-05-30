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

#ifndef DMAP_CACHE_BASE_H_
#define DMAP_CACHE_BASE_H_
#include <string>

namespace map_api {

/**
 * Allows transactions to register caches without needing to know the
 * types of a templated cache.
 */
class CacheBase {
  friend class Transaction;

 public:
  virtual ~CacheBase();

 private:
  virtual std::string underlyingTableName() const = 0;
  virtual void prepareForCommit() = 0;
  // This is necessary after a commit (and after chunk tracking resolution!) in
  // order to re-fetch
  // the correct metadata from the database.
  virtual void discardCachedInsertions() = 0;
  virtual void refreshAvailableIds() = 0;
  virtual size_t size() const = 0;
};

}  // namespace map_api

#endif  // DMAP_CACHE_BASE_H_
