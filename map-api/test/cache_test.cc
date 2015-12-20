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
  void initCacheView() {
    transaction_.reset(new Transaction);
    cache_ = transaction_->createCache<IntId, int>(table_);
  }

  std::shared_ptr<Transaction> transaction_;
  std::shared_ptr<ThreadsafeCache<IntId, int>> cache_;

  template <typename Type>
  struct TestData {
    template <int Index>
    static Type get();
  };

  typedef TestData<IntId> IdData;
  typedef TestData<int> IntData;
};

template <>
template <int Index>
IntId CacheTest::TestData<IntId>::get() {
  IntId id;
  static_assert(Index > 0, "0 or below would create an invalid id!");
  generateIdFromInt(Index, &id);
  return id;
}

template <>
template <int Index>
int CacheTest::TestData<int>::get() {
  return Index;
}

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
    EXPECT_FALSE(cache_->has(IdData::get<1>()));
    EXPECT_FALSE(cache_->has(IdData::get<2>()));
    EXPECT_FALSE(cache_->has(IdData::get<3>()));
    EXPECT_TRUE(cache_->insert(IdData::get<1>(), IntData::get<1>()));
    EXPECT_TRUE(transaction_->commit());
    IPC::barrier(INIT, 1);
    table_->shareAllChunks();
    IPC::barrier(ROOT_INSERTED, 1);
    IPC::barrier(A_DONE, 1);

    initCacheView();
    cache_->getAllAvailableIds(&id_result);
    EXPECT_EQ(2u, id_result.size());
    ASSERT_TRUE(cache_->has(IdData::get<1>()));
    ASSERT_TRUE(cache_->has(IdData::get<2>()));
    EXPECT_FALSE(cache_->has(IdData::get<3>()));
    // As changed by process A:
    EXPECT_EQ(IntData::get<3>(), cache_->get(IdData::get<1>()));
    EXPECT_EQ(IntData::get<2>(), cache_->get(IdData::get<2>()));
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    IPC::barrier(ROOT_INSERTED, 1);
    initCacheView();
    CHECK(cache_->has(IdData::get<1>()));
    cache_->getMutable(IdData::get<1>()) = IntData::get<3>();
    CHECK(cache_->insert(IdData::get<2>(), IntData::get<2>()));
    CHECK(transaction_->commit());
    table_->shareAllChunks();
    IPC::barrier(A_DONE, 1);
  }
}

TEST_F(CacheTest, hadBeenUpdatedBeforeThisTransaction) {
  initCacheView();
  EXPECT_TRUE(cache_->insert(IdData::get<1>(), IntData::get<1>()));
  EXPECT_TRUE(transaction_->commit());

  initCacheView();
  EXPECT_FALSE(cache_->hadBeenUpdatedBeforeThisTransaction(IdData::get<1>()));
  cache_->getMutable(IdData::get<1>()) = IntData::get<2>();
  EXPECT_TRUE(transaction_->commit());

  initCacheView();
  EXPECT_TRUE(cache_->hadBeenUpdatedBeforeThisTransaction(IdData::get<1>()));
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
