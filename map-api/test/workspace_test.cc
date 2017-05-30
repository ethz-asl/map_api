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

#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/transaction.h"
#include "map-api/workspace.h"
#include "./net_table_fixture.h"

namespace map_api {

class WorkspaceTest : public NetTableFixture {
  enum kTable2Fields {
    kParent
  };

 protected:
  virtual void SetUp() {
    NetTableFixture::SetUp();

    // Add a trackee table.
    std::shared_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName2);
    descriptor->addField<map_api_common::Id>(kParent);
    table_2_ = NetTableManager::instance().addTable(descriptor);
    table_2_->pushNewChunkIdsToTracker(table_, [](const Revision& revision) {
      map_api_common::Id result;
      revision.get(kParent, &result);
      return result;
    });

    // Commit two trackers and trackees.
    Transaction transaction;
    for (int i = 0; i < 2; ++i) {
      chunk_ = table_->newChunk();
      insert(42, &tracker_id_[i], &transaction);
      std::shared_ptr<Revision> revision = table_2_->getTemplate();
      revision->set(kParent, tracker_id_[i]);
      map_api_common::Id trackee_id;
      map_api_common::generateId(&trackee_id);
      revision->setId(trackee_id);
      transaction.insert(table_2_, table_2_->newChunk(), revision);
    }
    CHECK(transaction.commit());
  }

  size_t numAvailableIds(NetTable* table, Transaction* transaction) const {
    CHECK_NOTNULL(table);
    CHECK_NOTNULL(transaction);
    std::vector<map_api_common::Id> ids;
    transaction->getAvailableIds(table, &ids);
    return ids.size();
  }

  NetTable* table_2_;
  map_api_common::Id tracker_id_[2];

 private:
  const std::string kTableName2 = "workspace_test_table";
};

TEST_F(WorkspaceTest, FullTable) {
  Transaction t;
  EXPECT_EQ(2u, numAvailableIds(table_, &t));
  EXPECT_EQ(2u, numAvailableIds(table_2_, &t));
}

TEST_F(WorkspaceTest, BlackTable) {
  std::shared_ptr<Workspace> workspace(new Workspace({table_}, {}));
  Transaction t(workspace);
  EXPECT_EQ(0u, numAvailableIds(table_, &t));
  EXPECT_EQ(2u, numAvailableIds(table_2_, &t));
}

TEST_F(WorkspaceTest, WhiteTable) {
  std::shared_ptr<Workspace> workspace(new Workspace({}, {table_}));
  Transaction t(workspace);
  EXPECT_EQ(2u, numAvailableIds(table_, &t));
  EXPECT_EQ(0u, numAvailableIds(table_2_, &t));
}

TEST_F(WorkspaceTest, BlackWhiteTable) {
  std::shared_ptr<Workspace> workspace(
      new Workspace({table_}, {table_, table_2_}));
  Transaction t(workspace);
  EXPECT_EQ(0u, numAvailableIds(table_, &t));
  EXPECT_EQ(2u, numAvailableIds(table_2_, &t));
}

TEST_F(WorkspaceTest, BlackChunk) {
  std::shared_ptr<Workspace> workspace(new Workspace);
  Transaction t0;
  workspace->mergeRevisionTrackeesIntoBlacklist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t(workspace);
  EXPECT_EQ(1u, numAvailableIds(table_, &t));
  EXPECT_EQ(1u, numAvailableIds(table_2_, &t));
}

TEST_F(WorkspaceTest, WhiteChunk) {
  Transaction t0;

  std::shared_ptr<Workspace> workspace(new Workspace);
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t(workspace);
  EXPECT_EQ(1u, numAvailableIds(table_, &t));
  EXPECT_EQ(1u, numAvailableIds(table_2_, &t));

  // Check whether table blacklist works as well.
  workspace.reset(new Workspace({table_}, {}));
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t1(workspace);
  EXPECT_EQ(0u, numAvailableIds(table_, &t1));
  EXPECT_EQ(1u, numAvailableIds(table_2_, &t1));

  // Check whether table whitelist works as well.
  workspace.reset(new Workspace({}, {table_}));
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t2(workspace);
  EXPECT_EQ(1u, numAvailableIds(table_, &t2));
  EXPECT_EQ(0u, numAvailableIds(table_2_, &t2));

  // Check whether table black-white list works as well.
  workspace.reset(new Workspace({table_}, {table_, table_2_}));
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t3(workspace);
  EXPECT_EQ(0u, numAvailableIds(table_, &t3));
  EXPECT_EQ(1u, numAvailableIds(table_2_, &t3));
}

TEST_F(WorkspaceTest, BlackWhiteChunk) {
  Transaction t0;

  std::shared_ptr<Workspace> workspace(new Workspace);
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[0], table_), table_);
  workspace->mergeRevisionTrackeesIntoWhitelist(
      *t0.getById(tracker_id_[1], table_), table_);
  workspace->mergeRevisionTrackeesIntoBlacklist(
      *t0.getById(tracker_id_[0], table_), table_);
  Transaction t(workspace);
  EXPECT_EQ(1u, numAvailableIds(table_, &t));
  EXPECT_EQ(1u, numAvailableIds(table_2_, &t));
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
