#include <set>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/cache.h"
#include "map-api/chunk-manager.h"
#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "./net_table_fixture.h"

namespace map_api {

UNIQUE_ID_DEFINE_ID(IntId);
MAP_API_REVISION_UNIQUE_ID(IntId);

}  //  namespace map_api

UNIQUE_ID_DEFINE_ID_HASH(map_api::IntId);

namespace map_api {

template <>
std::shared_ptr<int> objectFromRevision(const Revision& revision) {
  std::shared_ptr<int> object(new int);
  revision.get(NetTableFixture::kFieldName, object.get());
  return object;
}
void objectToRevision(const int& object, Revision* revision) {
  CHECK_NOTNULL(revision)->set(NetTableFixture::kFieldName, object);
}
bool requiresUpdate(const int& object, const Revision& revision) {
  return !revision.verifyEqual(NetTableFixture::kFieldName, object);
}

typedef Cache<IntId, std::shared_ptr<int>> IntCache;

template <typename CacheOrTransaction>
struct IdType;

template <>
struct IdType<IntCache> {
  typedef IntId type;
};

template <>
struct IdType<Transaction> {
  typedef common::Id type;
};

template <typename CacheOrTransaction>
class CacheAndTransactionTest : public NetTableFixture {
 protected:
  static constexpr int kChunkSize = 1024;

  virtual void SetUp() {
    NetTableFixture::SetUp();
    typeSetUp();
  }

  void typeSetUp();

  void insert(const int to_insert,
              typename IdType<CacheOrTransaction>::type* id);
  void update(const int new_value,
              const typename IdType<CacheOrTransaction>::type& id);

  void remove(const typename IdType<CacheOrTransaction>::type& id);

  std::shared_ptr<Transaction> transaction_;
  std::shared_ptr<ChunkManagerChunkSize> manager_;
  std::shared_ptr<IntCache> cache_;
};

template <>
void CacheAndTransactionTest<Transaction>::typeSetUp() {
  transaction_.reset(new Transaction);
}

template <>
void CacheAndTransactionTest<IntCache>::typeSetUp() {
  transaction_.reset(new Transaction);
  manager_.reset(new ChunkManagerChunkSize(kChunkSize, table_));
  cache_.reset(new IntCache(transaction_, table_, manager_));
}

template <>
void CacheAndTransactionTest<Transaction>::insert(const int to_insert,
                                                  common::Id* id) {
  CHECK(transaction_);
  NetTableFixture::insert(to_insert, id, transaction_.get());
}

template <>
void CacheAndTransactionTest<IntCache>::insert(const int to_insert, IntId* id) {
  CHECK(cache_);
  generateId(CHECK_NOTNULL(id));
  CHECK(cache_->insert(*id, std::make_shared<int>(to_insert)));
}

template <>
void CacheAndTransactionTest<Transaction>::update(const int new_value,
                                                  const common::Id& id) {
  CHECK(transaction_);
  NetTableFixture::update(new_value, id, transaction_.get());
}

template <>
void CacheAndTransactionTest<IntCache>::update(const int new_value,
                                               const IntId& id) {
  CHECK(cache_);
  cache_->getMutable(id) = std::make_shared<int>(new_value);
}

template <>
void CacheAndTransactionTest<Transaction>::remove(const common::Id& id) {
  CHECK(transaction_);
  transaction_->remove(id, table_);
}

template <>
void CacheAndTransactionTest<IntCache>::remove(const IntId& id) {
  CHECK(cache_);
  cache_->erase(id);
}

typedef ::testing::Types<IntCache, Transaction> AllTypes;

TYPED_TEST_CASE(CacheAndTransactionTest, AllTypes);

TYPED_TEST(CacheAndTransactionTest, MultiCommit) {
  typename IdType<TypeParam>::type inserted_id_1, inserted_id_2;

  this->insert(1, &inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  EXPECT_EQ(1u, this->count());

  this->insert(2, &inserted_id_2);
  this->update(3, inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  EXPECT_EQ(2u, this->count());

  Transaction perturber;
  this->NetTableFixture::update(4, inserted_id_2, &perturber);
  EXPECT_TRUE(perturber.commit());

  this->remove(inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  EXPECT_EQ(1u, this->count());

  this->update(5, inserted_id_2);
  EXPECT_FALSE(this->transaction_->commit());
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
