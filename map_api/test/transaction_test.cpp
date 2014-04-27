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

TEST_F(TransactionTest, InsertBeforeTableInit){
  TransactionTestTable table(owner_);
  EXPECT_TRUE(transaction_.begin());
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table, table.sample(3.14)),
            Hash());
}

/**
 * Fixture for transaction tests with a cru table
 */
class TransactionCRUTest : public TransactionTest {
 public:
  TransactionCRUTest() : TransactionTest(), table_(owner_) {
    table_.init();
    transaction_.begin();
  }
 protected:
  virtual void TearDown() {
    transaction_.abort();
    table_.cleanup();
  }
  Hash insertSample(double sample){
    return transaction_.insert<CRUTableInterface>(
        table_, table_.sample(sample));
  }
  bool updateSample(const Hash& id, double newValue){
    return transaction_.update(table_, id, table_.sample(newValue));
  }
  bool verify(const Hash& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction_.read<CRUTableInterface>(
        table_, id);
    if (!row){
      LOG(ERROR) << "Failed to fetch row for verification";
      return false;
    }
    if (!row->get("n", &actual)){
      LOG(ERROR) << "Error when getting field 'n' from " << id.getString();
      return false;
    }
    // no margin of error needed - it's supposedly the same implementation
    if (actual != expected){
      LOG(ERROR) << "Actual is " << actual << " while expected is " << expected;
    }
    return actual == expected;
  }
  TransactionTestTable table_;
};

TEST_F(TransactionCRUTest, InsertNonsense){
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table_, nonsense), Hash());
}

TEST_F(TransactionCRUTest, InsertUpdateReadBeforeCommit){
  Hash id = insertSample(1.618);
  EXPECT_TRUE(verify(id, 1.618));
  EXPECT_TRUE(updateSample(id, 007));
  EXPECT_TRUE(verify(id, 007));
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
    return transaction.insert<CRUTableInterface>(table_, table_.sample(sample));
  }
  bool updateSample(Transaction& transaction, const Hash& id, double newValue){
    return transaction.update(table_, id, table_.sample(newValue));
  }
  bool verify(Transaction& transaction, const Hash& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction.read<CRUTableInterface>(
        table_, id);
    if (!row){
      LOG(ERROR) << "Failed to fetch row for verification";
      return false;
    }
    if (!row->get("n", &actual)){
      LOG(ERROR) << "Error when getting field 'n' from " << id.getString();
      return false;
    }
    // no margin of error needed - it's supposedly the same implementation
    return actual == expected;
  }
  TransactionTestTable table_;
};

TEST_F(MultiTransactionSingleCRUTest, SerialInsertRead) {
  // a
  Owner& a = addOwner();
  Transaction& at = a.beginNewTransaction();
  Hash ah = insertSample(at, 3.14);
  EXPECT_NE(ah, Hash());
  EXPECT_TRUE(at.commit());
  // b
  Owner& b = addOwner();
  Transaction& bt = b.beginNewTransaction();
  Hash bh = insertSample(bt, 42);
  EXPECT_NE(bh, Hash());
  EXPECT_TRUE(bt.commit());
  // read
  Owner& r = addOwner();
  Transaction& rt = r.beginNewTransaction();
  EXPECT_TRUE(verify(rt, ah, 3.14));
  EXPECT_TRUE(verify(rt, bh, 42));
  EXPECT_TRUE(rt.abort());
}

TEST_F(MultiTransactionSingleCRUTest, ParallelInsertRead) {
  Owner& a = addOwner(), &b = addOwner();
  Transaction& at = a.beginNewTransaction(), &bt = b.beginNewTransaction();
  Hash ah = insertSample(at, 3.14), bh = insertSample(bt, 42);
  EXPECT_NE(ah, Hash());
  EXPECT_NE(bh, Hash());
  EXPECT_TRUE(bt.commit());
  EXPECT_TRUE(at.commit());
  // read
  Owner& r = addOwner();
  Transaction& rt = r.beginNewTransaction();
  EXPECT_TRUE(verify(rt, ah, 3.14));
  EXPECT_TRUE(verify(rt, bh, 42));
  EXPECT_TRUE(rt.abort());
}

TEST_F(MultiTransactionSingleCRUTest, UpdateRead) {
  // Insert item to be updated
  Hash itemId;
  Owner& a = addOwner();
  Transaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(aInsert, 3.14);
  EXPECT_NE(itemId, Hash());
  EXPECT_TRUE(aInsert.commit());
  // a updates item and commits
  Transaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(aUpdate, itemId, 42));
  EXPECT_TRUE(aUpdate.commit());
  // check presence
  Transaction& aCheck = a.beginNewTransaction();
  EXPECT_TRUE(verify(aCheck, itemId, 42));
  EXPECT_TRUE(aCheck.abort());
}
