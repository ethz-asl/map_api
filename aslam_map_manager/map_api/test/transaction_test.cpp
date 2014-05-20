#include <list>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/map-api-core.h>
#include <map-api/transaction.h>

#include "test_table.cpp"

using namespace map_api;

/**
 * Fixture for simple transaction tests
 */
class TransactionTest : public testing::Test {
 protected:
  Transaction transaction_;
};

/**
 * CRU table for query tests TODO(tcies) test a CRTable
 */
class TransactionTestTable : public TestTable<CRUTableInterface> {
 public:
  std::shared_ptr<Revision> sample(double n){
    std::shared_ptr<Revision> revision = getTemplate();
    if (!revision->set(sampleField(), n)){
      LOG(ERROR) << "Failed to set " << sampleField();
      return std::shared_ptr<Revision>();
    }
    return revision;
  }
  inline static const std::string sampleField(){
    return "n";
  }
 protected:
  virtual const std::string name() const override {
    return "transaction_test_table";
  }
  virtual void define() {
    addField<double>(sampleField());
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
  TransactionTestTable table;
  system("cp database.db /tmp/trate0.db");
  EXPECT_TRUE(table.init());
  std::shared_ptr<Revision> data = table.sample(6.626e-34);
  EXPECT_EQ(Id(), transaction_.insert(table, data));
  // read and update should fail only because transaction hasn't started yet,
  // so we need to insert some data
  Transaction valid;
  EXPECT_TRUE(valid.begin());
  Id inserted = valid.insert(table, data);
  EXPECT_NE(inserted, Id());
  system("cp database.db /tmp/trate.db");
  EXPECT_TRUE(valid.commit());

  EXPECT_FALSE(transaction_.read<CRUTableInterface>(table, inserted));
  EXPECT_FALSE(transaction_.update(table, inserted, data));
}

TEST_F(TransactionTest, InsertBeforeTableInit){
  TransactionTestTable table;
  EXPECT_TRUE(transaction_.begin());
  EXPECT_DEATH(transaction_.insert(table, table.sample(3.14)), "^");
}

/**
 * Fixture for transaction tests with a cru table
 */
class TransactionCRUTest : public TransactionTest {
 protected:
  virtual void SetUp() {
    MapApiCore::getInstance().purgeDb();
    table_.init();
    transaction_.begin();
  }
  Id insertSample(double sample){
    return transaction_.insert(table_, table_.sample(sample));
  }
  bool updateSample(const Id& id, double newValue){
    return transaction_.update(table_, id, table_.sample(newValue));
  }
  void verify(const Id& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction_.read<CRUTableInterface>(
        table_, id);
    ASSERT_TRUE(static_cast<bool>(row));
    EXPECT_TRUE(row->get(table_.sampleField(), &actual));
    EXPECT_EQ(expected, actual);
  }
  TransactionTestTable table_;
};

TEST_F(TransactionCRUTest, InsertNonsense){
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_DEATH(transaction_.insert(table_, nonsense), "^");
}

TEST_F(TransactionCRUTest, InsertUpdateReadBeforeCommit){
  Id id = insertSample(1.618);
  verify(id, 1.618);
  EXPECT_TRUE(updateSample(id, 007));
  verify(id, 007);
}

/**
 * Fixture for tests with multiple owners and transactions
 */
class MultiTransactionTest : public testing::Test {
 protected:
  class Agent{
   public:
    Transaction& beginNewTransaction(){
      transactions_.push_back(Transaction());
      Transaction& current_transaction = transactions_.back();
      current_transaction.begin();
      return current_transaction;
    }
   private:
    std::list<Transaction> transactions_;
  };
  Agent& addAgent(){
    agents_.push_back(Agent());
    return agents_.back();
  }
  std::list<Agent> agents_;
};

/**
 * Fixture for multi-transaction tests on a single CRU table interface
 * TODO(tcies) multiple table interfaces (test definition sync)
 */
class MultiTransactionSingleCRUTest : public MultiTransactionTest {
 protected:
  virtual void SetUp()  {
    MapApiCore::getInstance().purgeDb();
    table_.init();
  }
  Id insertSample(Transaction& transaction, double sample){
    return transaction.insert(table_, table_.sample(sample));
  }
  bool updateSample(Transaction& transaction, const Id& id, double newValue){
    std::shared_ptr<Revision> toUpdate =
        transaction.read<CRUTableInterface>(table_, id);
    CHECK(toUpdate);
    toUpdate->set("n", newValue);
    return transaction.update(table_, id, toUpdate);
  }
  void verify(Transaction& transaction, const Id& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction.read<CRUTableInterface>(
        table_, id);
    ASSERT_TRUE(static_cast<bool>(row));
    EXPECT_TRUE(row->get(table_.sampleField(), &actual));
    EXPECT_EQ(expected, actual);
  }
  TransactionTestTable table_;
};

TEST_F(MultiTransactionSingleCRUTest, SerialInsertRead) {
  // Insert by a
  Agent& a = addAgent();
  Transaction& at = a.beginNewTransaction();
  Id aId = insertSample(at, 3.14);
  EXPECT_NE(aId, Id());
  EXPECT_TRUE(at.commit());
  // Insert by b
  Agent& b = addAgent();
  Transaction& bt = b.beginNewTransaction();
  Id bId = insertSample(bt, 42);
  EXPECT_NE(bId, Id());
  EXPECT_TRUE(bt.commit());
  // Check presence of samples in table
  Agent& verifier = addAgent();
  Transaction& verification = verifier.beginNewTransaction();
  verify(verification, aId, 3.14);
  verify(verification, bId, 42);
}

TEST_F(MultiTransactionSingleCRUTest, ParallelInsertRead) {
  Agent& a = addAgent(), &b = addAgent();
  Transaction& at = a.beginNewTransaction(), &bt = b.beginNewTransaction();
  Id aId = insertSample(at, 3.14), bId = insertSample(bt, 42);
  EXPECT_NE(aId, Id());
  EXPECT_NE(bId, Id());
  EXPECT_TRUE(bt.commit());
  EXPECT_TRUE(at.commit());
  // Check presence of samples in table
  Agent& verifier = addAgent();
  Transaction& verification = verifier.beginNewTransaction();
  verify(verification, aId, 3.14);
  verify(verification, bId, 42);
}

TEST_F(MultiTransactionSingleCRUTest, UpdateRead) {
  // Insert item to be updated
  Id itemId;
  Agent& a = addAgent();
  Transaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(aInsert, 3.14);
  EXPECT_NE(itemId, Id());
  EXPECT_TRUE(aInsert.commit());
  // a updates item and commits
  Transaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(aUpdate, itemId, 42));
  EXPECT_TRUE(aUpdate.commit());
  // Check presence of sample in table
  Transaction& aCheck = a.beginNewTransaction();
  verify(aCheck, itemId, 42);
  // adding another udpate and check to see whether multiple history entries
  // work together well
  Transaction& aUpdate2 = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(aUpdate2, itemId, 21));
  EXPECT_TRUE(aUpdate2.commit());
  // Check presence of sample in table
  Transaction& aCheck2 = a.beginNewTransaction();
  verify(aCheck2, itemId, 21);
}

TEST_F(MultiTransactionSingleCRUTest, ParallelUpdate) {
  // Insert item to be updated
  Id itemId;
  Agent& a = addAgent();
  Transaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(aInsert, 3.14);
  EXPECT_NE(itemId, Id());
  EXPECT_TRUE(aInsert.commit());
  // a updates item
  Transaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(aUpdate, itemId, 42));
  // b updates item
  Agent& b = addAgent();
  Transaction& bUpdate = b.beginNewTransaction();
  EXPECT_TRUE(updateSample(bUpdate, itemId, 12.34));
  // expect commit conflict
  EXPECT_TRUE(bUpdate.commit());
  EXPECT_FALSE(aUpdate.commit());
  // make sure b has won
  Transaction& aCheck = a.beginNewTransaction();
  verify(aCheck, itemId, 12.34);
}
