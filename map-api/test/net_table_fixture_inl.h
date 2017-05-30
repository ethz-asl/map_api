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

#ifndef DMAP_NET_TABLE_FIXTURE_INL_H_
#define DMAP_NET_TABLE_FIXTURE_INL_H_

#include <string>

#include <gtest/gtest.h>

#include <map-api/core.h>
#include <map-api/net-table.h>
#include <map-api/net-table-manager.h>
#include <map-api/net-table-transaction.h>
#include <map-api/transaction.h>
#include "./net_table_fixture.h"

namespace map_api {

void NetTableFixture::SetUp() {
  MapApiFixture::SetUp();
  std::shared_ptr<TableDescriptor> descriptor(new TableDescriptor);
  descriptor->setName(kTableName);
  descriptor->addField<int>(kFieldName);
  table_ = NetTableManager::instance().addTable(descriptor);
}

size_t NetTableFixture::count() {
  ConstRevisionMap results;
  table_->dumpActiveChunksAtCurrentTime(&results);
  return results.size();
}

void NetTableFixture::increment(const map_api_common::Id& id, ChunkBase* chunk,
                                NetTableTransaction* transaction) {
  CHECK_NOTNULL(chunk);
  CHECK_NOTNULL(transaction);
  ConstRevisionMap chunk_dump;
  transaction->dumpChunk(chunk, &chunk_dump);
  ConstRevisionMap::iterator found = chunk_dump.find(id);
  CHECK(found != chunk_dump.end());
  std::shared_ptr<Revision> to_update;
  found->second->copyForWrite(&to_update);
  int transient_value;
  to_update->get(kFieldName, &transient_value);
  ++transient_value;
  to_update->set(kFieldName, transient_value);
  transaction->update(to_update);
}

void NetTableFixture::increment(NetTable* table, const map_api_common::Id& id,
                                ChunkBase* chunk, Transaction* transaction) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  CHECK_NOTNULL(transaction);
  ConstRevisionMap chunk_dump;
  transaction->dumpChunk(table, chunk, &chunk_dump);
  ConstRevisionMap::iterator found = chunk_dump.find(id);
  std::shared_ptr<Revision> to_update;
  found->second->copyForWrite(&to_update);
  int transient_value;
  to_update->get(kFieldName, &transient_value);
  ++transient_value;
  to_update->set(kFieldName, transient_value);
  transaction->update(table, to_update);
}

map_api_common::Id NetTableFixture::insert(int value, ChunkBase* chunk) {
  map_api_common::Id insert_id;
  generateId(&insert_id);
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->setId(insert_id);
  to_insert->set(kFieldName, value);
  EXPECT_TRUE(table_->insert(LogicalTime::sample(), chunk, to_insert));
  return insert_id;
}

map_api_common::Id NetTableFixture::insert(
    int value, ChunkTransaction* transaction) {
  map_api_common::Id insert_id;
  generateId(&insert_id);
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->setId(insert_id);
  to_insert->set(kFieldName, value);
  transaction->insert(to_insert);
  return insert_id;
}

void NetTableFixture::insert(int value, map_api_common::Id* id,
                             Transaction* transaction) {
  map_api_common::Id id_obj;
  if (!id) {
    id = &id_obj;
  }
  CHECK_NOTNULL(transaction);
  generateId(id);
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->setId(*id);
  to_insert->set(kFieldName, value);
  transaction->insert(table_, chunk_, to_insert);
}

void NetTableFixture::insert(int value, const map_api_common::Id& id,
                             Transaction* transaction) {
  CHECK_NOTNULL(transaction);
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->setId(id);
  to_insert->set(kFieldName, value);
  transaction->insert(table_, chunk_, to_insert);
}

template <typename IdType>
void NetTableFixture::update(int new_value, const IdType& id,
                             Transaction* transaction) {
  CHECK_NOTNULL(transaction);
  std::shared_ptr<const Revision> to_update =
      transaction->getById(id, table_, chunk_);
  ASSERT_TRUE(to_update != nullptr);
  std::shared_ptr<Revision> update;
  to_update->copyForWrite(&update);
  update->set(kFieldName, new_value);
  transaction->update(table_, update);
}

const std::string NetTableFixture::kTableName = "chunk_test_table";

}  // namespace map_api

#endif  // DMAP_NET_TABLE_FIXTURE_INL_H_
