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
    if (!revision->set(sampleField(),n)){
      LOG(ERROR) << "Failed to set " << sampleField();
      return std::shared_ptr<Revision>();
    }
    return revision;
  }
  inline static const std::string sampleField(){
    return "n";
  }
 protected:
  virtual bool define() {
    addField<double>(sampleField());
    return true;
  }
};

TEST_F(TransactionTest, BeginAbort){
  EXPECT_TRUE(transaction_.begin());
  EXPECT_TRUE(transaction_.abort());
}

TEST_F(TransactionTest, BeginCommit){
  EXPECT_TRUE(transaction_.begin());
  EXPECT_FALSE(transaction_.commit());
}

TEST_F(TransactionTest, OperationsBeforeBegin){
  TransactionTestTable table(owner_);
  EXPECT_TRUE(table.init());
  std::shared_ptr<Revision> data = table.sample(6.626e-34);
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table, data), Hash());
  // read and update should fail only because transaction hasn't started yet,
  // so we need to insert some data
  Transaction valid(owner_);
  EXPECT_TRUE(valid.begin());
  Hash inserted = valid.insert<CRUTableInterface>(table, data);
  EXPECT_NE(inserted, Hash());
  EXPECT_TRUE(valid.commit());

  EXPECT_FALSE(transaction_.read<CRUTableInterface>(table, inserted));
  EXPECT_FALSE(transaction_.update(table, inserted, data));
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
  void verify(const Hash& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction_.read<CRUTableInterface>(
        table_, id);
    EXPECT_TRUE(static_cast<bool>(row)); // direct doesn't compile
    EXPECT_TRUE(row->get(table_.sampleField(), &actual));
    EXPECT_EQ(actual, expected);
  }
  TransactionTestTable table_;
};

TEST_F(TransactionCRUTest, InsertNonsense){
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_EQ(transaction_.insert<CRUTableInterface>(table_, nonsense), Hash());
}

TEST_F(TransactionCRUTest, InsertUpdateReadBeforeCommit){
  Hash id = insertSample(1.618);
  verify(id, 1.618);
  EXPECT_TRUE(updateSample(id, 007));
  verify(id, 007);
}

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
      Transaction& current_transaction = transactions_.back();
      current_transaction.begin();
      return current_transaction;
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
  void verify(Transaction& transaction, const Hash& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction.read<CRUTableInterface>(
        table_, id);
    EXPECT_TRUE(static_cast<bool>(row)); // direct doesn't compile
    EXPECT_TRUE(row->get(table_.sampleField(), &actual));
    EXPECT_EQ(actual, expected);
  }
  TransactionTestTable table_;
};

TEST_F(MultiTransactionSingleCRUTest, SerialInsertRead) {
  // Insert by a
  Owner& a = addOwner();
  Transaction& at = a.beginNewTransaction();
  Hash ah = insertSample(at, 3.14);
  EXPECT_NE(ah, Hash());
  EXPECT_TRUE(at.commit());
  // Insert by b
  Owner& b = addOwner();
  Transaction& bt = b.beginNewTransaction();
  Hash bh = insertSample(bt, 42);
  EXPECT_NE(bh, Hash());
  EXPECT_TRUE(bt.commit());
  // Check presence of samples in table
  Owner& verifier = addOwner();
  Transaction& verification = verifier.beginNewTransaction();
  verify(verification, ah, 3.14);
  verify(verification, bh, 42);
  EXPECT_TRUE(verification.abort());
}

TEST_F(MultiTransactionSingleCRUTest, ParallelInsertRead) {
  Owner& a = addOwner(), &b = addOwner();
  Transaction& at = a.beginNewTransaction(), &bt = b.beginNewTransaction();
  Hash ah = insertSample(at, 3.14), bh = insertSample(bt, 42);
  EXPECT_NE(ah, Hash());
  EXPECT_NE(bh, Hash());
  EXPECT_TRUE(bt.commit());
  EXPECT_TRUE(at.commit());
  // Check presence of samples in table
  Owner& verifier = addOwner();
  Transaction& verification = verifier.beginNewTransaction();
  verify(verification, ah, 3.14);
  verify(verification, bh, 42);
  EXPECT_TRUE(verification.abort());
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
  // Check presence of sample in table
  Transaction& aCheck = a.beginNewTransaction();
  verify(aCheck, itemId, 42);
  EXPECT_TRUE(aCheck.abort());
}

TEST_F(MultiTransactionSingleCRUTest, ParallelUpdate) {
  // Insert item to be updated
  Hash itemId;
  Owner& a = addOwner();
  Transaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(aInsert, 3.14);
  EXPECT_NE(itemId, Hash());
  EXPECT_TRUE(aInsert.commit());
  // a updates item
  Transaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(aUpdate, itemId, 42));
  // b updates item
  Owner& b = addOwner();
  Transaction& bUpdate = b.beginNewTransaction();
  EXPECT_TRUE(updateSample(bUpdate, itemId, 0xDEADBEEF));
  // expect commit conflict
  EXPECT_TRUE(bUpdate.commit());
  EXPECT_FALSE(aUpdate.commit());
  // make sure b has won
  Transaction& aCheck = a.beginNewTransaction();
  verify(aCheck, itemId, 0xDEADBEEF);
  EXPECT_TRUE(aCheck.abort());
}
