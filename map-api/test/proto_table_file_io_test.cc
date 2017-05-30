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

#include <fstream>  // NOLINT
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/ipc.h"
#include "map-api/net-table-transaction.h"
#include "map-api/proto-table-file-io.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/transaction.h"
#include "./net_table_fixture.h"

using namespace map_api;  // NOLINT

class ProtoTableFileIOTest : public NetTableFixture {};

TEST_F(ProtoTableFileIOTest, SaveAndRestoreFromFile) {
  ChunkBase* chunk = table_->newChunk();
  CHECK_NOTNULL(chunk);
  map_api_common::Id chunk_id = chunk->id();
  map_api_common::Id item_1_id;
  generateId(&item_1_id);
  map_api_common::Id item_2_id;
  generateId(&item_2_id);
  {
    Transaction transaction;
    std::shared_ptr<Revision> to_insert_1 = table_->getTemplate();
    to_insert_1->setId(item_1_id);
    to_insert_1->set(kFieldName, 42);
    std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
    to_insert_2->setId(item_2_id);
    to_insert_2->set(kFieldName, 21);
    transaction.insert(table_, chunk, to_insert_1);
    transaction.insert(table_, chunk, to_insert_2);
    ASSERT_TRUE(transaction.commit());
    ConstRevisionMap retrieved;
    LogicalTime dumptime = LogicalTime::sample();
    chunk->dumpItems(dumptime, &retrieved);
    ASSERT_EQ(2u, retrieved.size());
    ConstRevisionMap::iterator it = retrieved.find(item_1_id);
    ASSERT_FALSE(it == retrieved.end());
    LogicalTime time_1, time_2;
    int item_1, item_2;
    time_1 = it->second->getInsertTime();
    it->second->get(kFieldName, &item_1);
    it = retrieved.find(item_2_id);
    ASSERT_FALSE(it == retrieved.end());
    time_2 = it->second->getInsertTime();
    it->second->get(kFieldName, &item_2);
    EXPECT_EQ(time_1.serialize(), time_2.serialize());
    EXPECT_EQ(item_1, 42);
    EXPECT_EQ(item_2, 21);
  }

  const std::string test_filename = "./test_dump.table";
  // Drop all existing contents.
  std::fstream file;
  file.open(test_filename, std::fstream::binary | std::fstream::in |
                               std::fstream::out | std::fstream::trunc);

  {
    ProtoTableFileIO file_io(test_filename, table_);
    EXPECT_TRUE(file_io.storeTableContents(LogicalTime::sample()));
  }

  // Reset the state of the database.
  TearDown();
  SetUp();

  {
    ProtoTableFileIO file_io(test_filename, table_);
    ASSERT_TRUE(file_io.restoreTableContents());
  }

  {
    chunk = table_->getChunk(chunk_id);
    ASSERT_FALSE(chunk == nullptr);
    ConstRevisionMap retrieved;
    LogicalTime time_1, time_2;
    int item_1, item_2;
    LogicalTime dumptime = LogicalTime::sample();
    chunk->dumpItems(dumptime, &retrieved);
    ASSERT_EQ(2u, retrieved.size());
    ConstRevisionMap::iterator it = retrieved.find(item_1_id);
    ASSERT_FALSE(it == retrieved.end());
    time_1 = it->second->getInsertTime();
    it->second->get(kFieldName, &item_1);
    EXPECT_EQ(item_1, 42);

    it = retrieved.find(item_2_id);
    ASSERT_FALSE(it == retrieved.end());
    time_2 = it->second->getInsertTime();
    it->second->get(kFieldName, &item_2);
    EXPECT_EQ(item_2, 21);

    EXPECT_EQ(time_1.serialize(), time_2.serialize());
  }
}

MAP_API_UNITTEST_ENTRYPOINT
