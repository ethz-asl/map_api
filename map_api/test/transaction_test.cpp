/*
 * transaction_test.cpp
 *
 *  Created on: Apr 14, 2014
 *      Author: titus
 */
#include <list>

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

TEST_F(TransactionCRUTest, InsertNonsense){
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_TRUE(transaction_.begin());
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table_, nonsense), Hash());
}

TEST_F(TransactionTest, InsertBeforeTableInit){
  TransactionTestTable table(owner_);
  EXPECT_TRUE(transaction_.begin());
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table, table.sample(3.14)),
            Hash());
}

// TODO (tcies) access uninitialized transaction

/**
 * Fixture for tests with multiple owners and transactions
 */
class MultiTransactionTest : public testing::Test {
 protected:
  class Owner{
   public:
    Owner() : id_(Hash::randomHash()), transactions_() {}
    Transaction& beginNewTransaction(){
      transactions_.push_back(Transaction(id_));
      transactions_.back().begin();
      return transactions_.back();
    }
   private:
    Hash id_;
    std::list<Transaction> transactions_;
  };
  Owner& addOwner(){
    owners_.push_back(Owner());
    return owners_.back();
  }
  std::list<Owner> owners_;
};

/**
 * Fixture for multi-transaction tests on a single CRU table interface
 * TODO(tcies) multiple table interfaces (test definition sync)
 */
class MultiTransactionSingleCRUTest : public MultiTransactionTest {
 public:
  MultiTransactionSingleCRUTest() : MultiTransactionTest(),
  table_(Hash::randomHash()) {
    table_.init();
  }
 protected:
  virtual void TearDown() {
    table_.cleanup();
  }
  Hash insertSample(Transaction& transaction, double sample){
    return transaction.insert<CRUTableInterface>(table_, table_.sample(3.14));
  }
  TransactionTestTable table_;
};

TEST_F(MultiTransactionSingleCRUTest, SerialInsert) {
  Owner& a = addOwner();
  Transaction& at = a.beginNewTransaction();
  EXPECT_NE(insertSample(at, 3.14), Hash());
  EXPECT_TRUE(at.commit());
  Owner& b = addOwner();
  Transaction& bt = b.beginNewTransaction();
  EXPECT_NE(insertSample(bt, 42), Hash());
  EXPECT_TRUE(bt.commit());
  // system("cp database.db /tmp/database.db");
  // TODO(tcies) verify presence of data after finishing commit()
}

TEST_F(MultiTransactionSingleCRUTest, ParallelInsert) {
  Owner& a = addOwner(), &b = addOwner();
  Transaction& at = a.beginNewTransaction(), &bt = b.beginNewTransaction();
  EXPECT_NE(insertSample(at, 3.14), Hash());
  EXPECT_NE(insertSample(bt, 42), Hash());
  EXPECT_TRUE(bt.commit());
  EXPECT_TRUE(at.commit());
  // TODO(tcies) verify presence of data after finishing commit()
}

TEST_F(MultiTransactionSingleCRUTest, SerialUpdate) {
  // Prepare item to be updated
  Hash itemId;
  Owner& a = addOwner();
  Transaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(aInsert, 3.14);
  EXPECT_NE(itemId, Hash());
  EXPECT_TRUE(aInsert.commit());
  // a updates item and commits
  // b updates item and commits
  // TODO(tcies) first need to finish implementing commit
}
