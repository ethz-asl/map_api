/*
 * transaction_test.cpp
 *
 *  Created on: Apr 14, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/transaction.h>

#include "test_table.cpp"

using namespace map_api;

/**
 * Fixture for simple transaction tests
 */
class TransactionTest : public testing::Test {
 public:
  TransactionTest() : owner_(Hash::randomHash()), transaction_(owner_) {}
 protected:
  Hash owner_;
  Transaction transaction_;
};

TEST_F(TransactionTest, BeginAbort){
  EXPECT_TRUE(transaction_.begin());
  EXPECT_TRUE(transaction_.abort());
}

TEST_F(TransactionTest, BeginCommit){
  Hash owner = Hash::randomHash();
  Transaction transaction(owner);
  EXPECT_TRUE(transaction_.begin());
  EXPECT_FALSE(transaction_.commit());
}

/**
 * CRU table for query tests TODO(tcies) test a CRTable
 */
class TransactionTestTable : public TestTable {
 public:
  TransactionTestTable(const Hash& owner) : TestTable(owner) {}
  std::shared_ptr<Revision> sample(double n){
    std::shared_ptr<Revision> revision = getTemplate();
    if (!revision->set("n",n)){
      LOG(ERROR) << "Failed to set n";
      return std::shared_ptr<Revision>();
    }
    return revision;
  }
 protected:
  virtual bool define() {
    addField<double>("n");
    return true;
  }
};

/**
 * Fixture for transaction tests with a cru table
 */
class TransactionCRUTest : public TransactionTest {
 public:
  TransactionCRUTest() : TransactionTest(), table_(owner_) {
    table_.init();
  }
 protected:
  virtual void TearDown() {
    table_.cleanup();
  }
  TransactionTestTable table_;
};

TEST_F(TransactionCRUTest, QueueInsertNonsense){
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_TRUE(transaction_.begin());
  EXPECT_FALSE(transaction_.insert<CRUTableInterface>(table_, nonsense));
}

TEST_F(TransactionTest, QueueInsertBeforeTableInit){
  TransactionTestTable table(owner_);
  EXPECT_TRUE(transaction_.begin());
  EXPECT_FALSE(transaction_.insert<CRUTableInterface>(
      table, table.sample(3.14)));
}

// TODO (tcies) access uninitialized transaction
