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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#include <set>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api-common/unique-id.h>

#include "map-api/chunk-manager.h"
#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/threadsafe-cache.h"
#include "./net_table_fixture.h"

namespace map_api {

UNIQUE_ID_DEFINE_ID(IntId);
MAP_API_REVISION_UNIQUE_ID(IntId);

}  //  namespace map_api

UNIQUE_ID_DEFINE_ID_HASH(map_api::IntId);

namespace map_api {

template <>
void objectFromRevision(const Revision& revision, int* result) {
  CHECK_NOTNULL(result);
  CHECK(revision.hasField(NetTableFixture::kFieldName));
  revision.get(NetTableFixture::kFieldName, result);
}
void objectToRevision(const int& object, Revision* revision) {
  CHECK_NOTNULL(revision)->set(NetTableFixture::kFieldName, object);
}
bool requiresUpdate(const int& object, const Revision& revision) {
  return !revision.verifyEqual(NetTableFixture::kFieldName, object);
}

typedef ThreadsafeCache<IntId, int> IntCache;

template <typename CacheOrTransaction>
struct IdType;

template <>
struct IdType<IntCache> {
  typedef IntId type;
};

template <>
struct IdType<Transaction> {
  typedef map_api_common::Id type;
};

template <typename CacheOrTransaction>
class CacheAndTransactionTest : public NetTableFixture {
 protected:
  virtual void SetUp() {
    NetTableFixture::SetUp();
    transaction_.reset(new Transaction);
    typeSetUp();
  }

  void typeSetUp();

  void insert(const int to_insert,
              typename IdType<CacheOrTransaction>::type* id);
  void insert(const typename IdType<CacheOrTransaction>::type& id,
              const int to_insert);
  void update(const int new_value,
              const typename IdType<CacheOrTransaction>::type& id);

  void remove(const typename IdType<CacheOrTransaction>::type& id);

  typedef NetTableTransactionInterface<
      typename IdType<CacheOrTransaction>::type> InterfaceType;

  std::shared_ptr<Transaction> transaction_;
  std::shared_ptr<IntCache> cache_;
};

template <>
void CacheAndTransactionTest<Transaction>::typeSetUp() {
  chunk_ = table_->newChunk();
}

template <>
void CacheAndTransactionTest<IntCache>::typeSetUp() {
  cache_ = transaction_->createCache<IntId, int>(table_);
  chunk_ = cache_->chunk_manager_.getChunkForItem(*table_->getTemplate());
}

template <>
void CacheAndTransactionTest<Transaction>::insert(const int to_insert,
                                                  map_api_common::Id* id) {
  CHECK(transaction_);
  NetTableFixture::insert(to_insert, id, transaction_.get());
}

template <>
void CacheAndTransactionTest<IntCache>::insert(const int to_insert, IntId* id) {
  CHECK(cache_);
  generateId(CHECK_NOTNULL(id));
  CHECK(cache_->insert(*id, to_insert));
}

template <>
void CacheAndTransactionTest<Transaction>::insert(const typename map_api_common::Id& id,
                                                  const int to_insert) {
  CHECK(transaction_);
  NetTableFixture::insert(to_insert, id, transaction_.get());
}

template <>
void CacheAndTransactionTest<IntCache>::insert(const IntId& id,
                                               const int to_insert) {
  CHECK(cache_);
  CHECK(cache_->insert(id, to_insert));
}

template <>
void CacheAndTransactionTest<Transaction>::update(const int new_value,
                                                  const map_api_common::Id& id) {
  CHECK(transaction_);
  NetTableFixture::update(new_value, id, transaction_.get());
}

template <>
void CacheAndTransactionTest<IntCache>::update(const int new_value,
                                               const IntId& id) {
  CHECK(cache_);
  cache_->getMutable(id) = new_value;
}

template <>
void CacheAndTransactionTest<Transaction>::remove(const map_api_common::Id& id) {
  CHECK(transaction_);
  transaction_->remove(id, table_);
}

template <>
void CacheAndTransactionTest<IntCache>::remove(const IntId& id) {
  CHECK(cache_);
  cache_->erase(id);
}

typedef ::testing::Types<Transaction, IntCache> AllTypes;

TYPED_TEST_CASE(CacheAndTransactionTest, AllTypes);

TYPED_TEST(CacheAndTransactionTest, MultiCommit) {
  typename IdType<TypeParam>::type inserted_id_1, inserted_id_2;

  this->insert(1, &inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  ASSERT_EQ(1u, this->count());

  this->insert(2, &inserted_id_2);
  this->update(3, inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  ASSERT_EQ(2u, this->count());

  // The perturber introduces a conflict for a test below.
  Transaction perturber;
  this->NetTableFixture::update(4, inserted_id_2, &perturber);
  EXPECT_TRUE(perturber.commit());

  this->remove(inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  EXPECT_EQ(1u, this->count());

  // This should fail because of the conflict introduced by the perturber.
  this->update(5, inserted_id_2);
  EXPECT_FALSE(this->transaction_->commit());
}

TYPED_TEST(CacheAndTransactionTest, InsertErase) {
  typename IdType<TypeParam>::type inserted_id_1;

  this->insert(1, &inserted_id_1);
  this->remove(inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  ASSERT_EQ(0u, this->count());
}

TYPED_TEST(CacheAndTransactionTest, InsertUpdate) {
  typename IdType<TypeParam>::type inserted_id_1;

  this->insert(1, &inserted_id_1);
  this->update(2, inserted_id_1);
  EXPECT_TRUE(this->transaction_->commit());
  ASSERT_EQ(1u, this->count());
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
