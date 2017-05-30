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

#ifndef MAP_API_NET_TABLE_FIXTURE_H_
#define MAP_API_NET_TABLE_FIXTURE_H_

#include <string>

#include <gtest/gtest.h>

#include <map-api/net-table.h>
#include <map-api/net-table-transaction.h>
#include <map-api/transaction.h>
#include "./map_api_fixture.h"

namespace map_api {

class NetTableFixture : public MapApiFixture {
 public:
  enum Fields {
    kFieldName
  };

  virtual ~NetTableFixture() {}

 protected:
  virtual void SetUp();

  size_t count();

  void increment(const map_api_common::Id& id, ChunkBase* chunk,
                 NetTableTransaction* transaction);
  void increment(NetTable* table, const map_api_common::Id& id, ChunkBase* chunk,
                 Transaction* transaction);

  map_api_common::Id insert(int n, ChunkBase* chunk);
  map_api_common::Id insert(int n, ChunkTransaction* transaction);
  void insert(int n, map_api_common::Id* id, Transaction* transaction);
  void insert(int n, const map_api_common::Id& id, Transaction* transaction);

  template <typename IdType>
  void update(int n, const IdType& id, Transaction* transaction);

  static const std::string kTableName;
  NetTable* table_;
  ChunkBase* chunk_;               // generic chunk pointer for custom use
  map_api_common::Id chunk_id_, item_id_;  // equally generic
};

}  // namespace map_api

#include "./net_table_fixture_inl.h"

#endif  // MAP_API_NET_TABLE_FIXTURE_H_
