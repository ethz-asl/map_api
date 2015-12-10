#include <set>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/chunk-manager.h"
#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/threadsafe-cache.h"
#include "./net_table_fixture.h"

namespace map_api {

UNIQUE_ID_DEFINE_ID(IntId);
MAP_API_REVISION_UNIQUE_ID(IntId);

template <>
void objectFromRevision(const Revision& revision, int* result) {
  CHECK_NOTNULL(result);
  revision.get(NetTableFixture::kFieldName, result);
}
void objectToRevision(const int& object, Revision* revision) {
  CHECK_NOTNULL(revision)->set(NetTableFixture::kFieldName, object);
}
bool requiresUpdate(const int& object, const Revision& revision) {
  return !revision.verifyEqual(NetTableFixture::kFieldName, object);
}

}  // namespace map_api

UNIQUE_ID_DEFINE_ID_HASH(map_api::IntId);

namespace map_api {

class CacheTest : public NetTableFixture {
 protected:
  virtual void SetUp() {
    NetTableFixture::SetUp();
    for (int i = 0; i < 3; ++i) {
      generateIdFromInt(i + 1, &int_id_[i]);
      int_val_[i] = i;
    }
  }

  void initCacheView() {
    transaction_.reset(new Transaction);
    cache_ = transaction_->createCache<IntId, int>(table_);
  }

  std::shared_ptr<Transaction> transaction_;
  std::shared_ptr<ThreadsafeCache<IntId, int>> cache_;

  IntId int_id_[3];
  int int_val_[3];
};

TEST_F(CacheTest, GeneralTest) {
  enum SubProcesses {
    ROOT,
    A
  };
  enum Barriers {
    INIT,
    ROOT_INSERTED,
    A_DONE
  };

  std::vector<IntId> id_result;
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    initCacheView();
    cache_->getAllAvailableIds(&id_result);
    EXPECT_TRUE(id_result.empty());
    for (int i = 0; i < 3; ++i) {
      EXPECT_FALSE(cache_->has(int_id_[i]));
    }
    EXPECT_TRUE(cache_->insert(int_id_[0], int_val_[0]));
    EXPECT_TRUE(transaction_->commit());
    IPC::barrier(INIT, 1);
    table_->shareAllChunks();
    IPC::barrier(ROOT_INSERTED, 1);
    IPC::barrier(A_DONE, 1);

    initCacheView();
    cache_->getAllAvailableIds(&id_result);
    EXPECT_EQ(2u, id_result.size());
    ASSERT_TRUE(cache_->has(int_id_[0]));
    ASSERT_TRUE(cache_->has(int_id_[1]));
    EXPECT_FALSE(cache_->has(int_id_[2]));
    EXPECT_EQ(int_val_[2], cache_->get(int_id_[0]));
    EXPECT_EQ(int_val_[1], cache_->get(int_id_[1]));
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    IPC::barrier(ROOT_INSERTED, 1);
    initCacheView();
    CHECK(cache_->has(int_id_[0]));
    cache_->getMutable(int_id_[0]) = int_val_[2];
    CHECK(cache_->insert(int_id_[1], int_val_[1]));
    CHECK(transaction_->commit());
    table_->shareAllChunks();
    IPC::barrier(A_DONE, 1);
  }
}

TEST_F(CacheTest, HadBeenUpdatedAtBeginTime) {
  initCacheView();
  EXPECT_TRUE(cache_->insert(int_id_[0], int_val_[0]));
  EXPECT_TRUE(transaction_->commit());

  initCacheView();
  EXPECT_FALSE(cache_->hadBeenUpdatedAtBeginTime(int_id_[0]));
  cache_->getMutable(int_id_[0]) = int_val_[1];
  EXPECT_TRUE(transaction_->commit());

  initCacheView();
  EXPECT_TRUE(cache_->hadBeenUpdatedAtBeginTime(int_id_[0]));
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
