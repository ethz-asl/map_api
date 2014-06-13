#include <list>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/cru-table-ram-cache.h"
#include "map-api/map-api-core.h"
#include "map-api/local-transaction.h"

#include "test_table.cpp"

using namespace map_api;

/**
 * CRU table for query tests TODO(tcies) test a CRTable
 */
class TransactionTestTable {
 public:
  static CRUTableRAMCache& instance() {
    static CRUTableRAMCache table;
    if (!table.isInitialized()) {
      std::unique_ptr<map_api::TableDescriptor> descriptor(
          new map_api::TableDescriptor);
      descriptor->setName("transaction_test_table");
      descriptor->addField<double>(sampleField());
      table.init(&descriptor);
    }
    return table;
  }
  // TODO(tcies) un-hack once table-manager
  static void init() {
    std::unique_ptr<map_api::TableDescriptor> descriptor(
        new map_api::TableDescriptor);
    descriptor->setName("transaction_test_table");
    descriptor->addField<double>(sampleField());
    instance().init(&descriptor);
  }
  inline static const std::string sampleField(){
    return "n";
  }
};

/**
 * Fixture for simple transaction tests
 */
class TransactionTest : public testing::Test, protected CoreTester {
 protected:
  virtual void SetUp() override {
    resetDb();
  }
  std::shared_ptr<Revision> sample(double n, CRUTableRAMCache* table) {
    CHECK_NOTNULL(table);
    std::shared_ptr<Revision> revision = table->getTemplate();
    if (!revision->set(TransactionTestTable::sampleField(), n)){
      LOG(ERROR) << "Failed to set " << TransactionTestTable::sampleField();
      return std::shared_ptr<Revision>();
    }
    return revision;
  }
  LocalTransaction transaction_;
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
  CRUTableRAMCache* table = &TransactionTestTable::instance();
  std::shared_ptr<Revision> data = sample(6.626e-34, table);
  EXPECT_EQ(Id(), transaction_.insert(data, table));
  // read and update should fail only because transaction hasn't started yet,
  // so we need to insert some data
  LocalTransaction valid;
  EXPECT_TRUE(valid.begin());
  Id inserted = valid.insert(data, table);
  EXPECT_NE(inserted, Id());
  EXPECT_TRUE(valid.commit());

  EXPECT_FALSE(transaction_.read(inserted, table));
  EXPECT_FALSE(transaction_.update(inserted, data, table));
}

TEST_F(TransactionTest, InsertBeforeTableInit){
  CRUTableRAMCache table;
  EXPECT_TRUE(transaction_.begin());
  EXPECT_DEATH(transaction_.insert(sample(3.14, &table), &table), "^");
}

/**
 * Fixture for transaction tests with a cru table
 */
class TransactionCRUTest : public TransactionTest {
 protected:
  virtual void SetUp() {
    resetDb();
    table_ = &TransactionTestTable::instance();
    transaction_.begin();
  }
  Id insertSample(double n){
    return transaction_.insert(sample(n, table_), table_);
  }
  bool updateSample(const Id& id, double newValue){
    std::shared_ptr<Revision> revision = transaction_.read(id, table_);
    EXPECT_TRUE(static_cast<bool>(revision));
    revision->set(TransactionTestTable::sampleField(), newValue);
    return transaction_.update(id, revision, table_);
  }
  void verify(const Id& id, double expected){
    double actual;
    std::shared_ptr<Revision> row = transaction_.read(id, table_);
    ASSERT_TRUE(static_cast<bool>(row));
    EXPECT_TRUE(row->get(TransactionTestTable::sampleField(), &actual));
    EXPECT_EQ(expected, actual);
  }
  CRUTableRAMCache* table_;
};

TEST_F(TransactionCRUTest, InsertNonsense){
  std::shared_ptr<Revision> nonsense(new Revision());
  EXPECT_DEATH(transaction_.insert(nonsense, table_), "^");
}

TEST_F(TransactionCRUTest, InsertUpdateReadBeforeCommit){
  Id id = insertSample(1.618);
  verify(id, 1.618);
  EXPECT_TRUE(updateSample(id, 007));
  verify(id, 007);
}

/**
 * Fixture for tests with multiple agents and transactions
 */
class MultiTransactionTest : public testing::Test {
 protected:
  class Agent{
   public:
    LocalTransaction& beginNewTransaction(){
      transactions_.push_back(LocalTransaction());
      LocalTransaction& current_transaction = transactions_.back();
      current_transaction.begin();
      return current_transaction;
    }
   private:
    std::list<LocalTransaction> transactions_;
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
class MultiTransactionSingleCRUTest : public MultiTransactionTest,
protected CoreTester {
 protected:
  virtual void SetUp()  {
    resetDb();
    TransactionTestTable::init();
    table_ = &TransactionTestTable::instance();
  }

  std::shared_ptr<Revision> sample(double n, CRUTableRAMCache* table) {
    CHECK_NOTNULL(table);
    std::shared_ptr<Revision> revision = table->getTemplate();
    if (!revision->set(TransactionTestTable::sampleField(), n)){
      LOG(ERROR) << "Failed to set " << TransactionTestTable::sampleField();
      return std::shared_ptr<Revision>();
    }
    return revision;
  }

  Id insertSample(double n, LocalTransaction* transaction){
    return transaction->insert(sample(n, table_), table_);
  }

  bool updateSample(const Id& id, double newValue,
                    LocalTransaction* transaction){
    std::shared_ptr<Revision> toUpdate = transaction->read(id, table_);
    CHECK(toUpdate);
    toUpdate->set("n", newValue);
    return transaction->update(id, toUpdate, table_);
  }
  void verify(const Id& id, double expected, LocalTransaction* transaction){
    double actual;
    std::shared_ptr<Revision> row = transaction->read(id, table_);
    ASSERT_TRUE(static_cast<bool>(row));
    EXPECT_TRUE(row->get(TransactionTestTable::sampleField(), &actual));
    EXPECT_EQ(expected, actual);
  }
  void dump(LocalTransaction& transaction) {
    dump_set_.clear();
    std::unordered_map<Id, std::shared_ptr<Revision> > dump;
    transaction.dumpTable(table_, &dump);
    for (const std::pair<Id, std::shared_ptr<Revision> >& item : dump) {
      double value;
      item.second->get(TransactionTestTable::sampleField(), &value);
      dump_set_.insert(value);
    }
  }
  CRUTableRAMCache* table_;
  std::set<double> dump_set_;
};

TEST_F(MultiTransactionSingleCRUTest, SerialInsertRead) {
  // Insert by a
  Agent& a = addAgent();
  LocalTransaction& at = a.beginNewTransaction();
  Id aId = insertSample(3.14, &at);
  EXPECT_NE(aId, Id());
  EXPECT_TRUE(at.commit());
  // Insert by b
  Agent& b = addAgent();
  LocalTransaction& bt = b.beginNewTransaction();
  Id bId = insertSample(42, &bt);
  EXPECT_NE(bId, Id());
  EXPECT_TRUE(bt.commit());
  // Check presence of samples in table
  Agent& verifier = addAgent();
  LocalTransaction& verification = verifier.beginNewTransaction();
  verify(aId, 3.14, &verification);
  verify(bId, 42, &verification);
}

TEST_F(MultiTransactionSingleCRUTest, ParallelInsertRead) {
  Agent& a = addAgent(), &b = addAgent();
  LocalTransaction& at = a.beginNewTransaction(), &bt = b.beginNewTransaction();
  Id aId = insertSample(3.14, &at), bId = insertSample(42, &bt);
  EXPECT_NE(aId, Id());
  EXPECT_NE(bId, Id());
  EXPECT_TRUE(bt.commit());
  EXPECT_TRUE(at.commit());
  // Check presence of samples in table
  Agent& verifier = addAgent();
  LocalTransaction& verification = verifier.beginNewTransaction();
  verify(aId, 3.14, &verification);
  verify(bId, 42, &verification);
}

TEST_F(MultiTransactionSingleCRUTest, UpdateRead) {
  // Insert item to be updated
  Id itemId;
  Agent& a = addAgent();
  LocalTransaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(3.14, &aInsert);
  EXPECT_NE(itemId, Id());
  EXPECT_TRUE(aInsert.commit());
  // a updates item and commits
  LocalTransaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(itemId, 42, &aUpdate));
  EXPECT_TRUE(aUpdate.commit());
  // Check presence of sample in table
  LocalTransaction& aCheck = a.beginNewTransaction();
  verify(itemId, 42, &aCheck);
  // adding another udpate and check to see whether multiple history entries
  // work together well
  LocalTransaction& aUpdate2 = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(itemId, 21, &aUpdate2));
  EXPECT_TRUE(aUpdate2.commit());
  // Check presence of sample in table
  LocalTransaction& aCheck2 = a.beginNewTransaction();
  verify(itemId, 21, &aCheck2);
}

TEST_F(MultiTransactionSingleCRUTest, ParallelUpdate) {
  // Insert item to be updated
  Id itemId;
  Agent& a = addAgent();
  LocalTransaction& aInsert = a.beginNewTransaction();
  itemId = insertSample(3.14, &aInsert);
  EXPECT_NE(itemId, Id());
  EXPECT_TRUE(aInsert.commit());
  // a updates item
  LocalTransaction& aUpdate = a.beginNewTransaction();
  EXPECT_TRUE(updateSample(itemId, 42, &aUpdate));
  // b updates item
  Agent& b = addAgent();
  LocalTransaction& bUpdate = b.beginNewTransaction();
  EXPECT_TRUE(updateSample(itemId, 12.34, &bUpdate));
  // expect commit conflict
  EXPECT_TRUE(bUpdate.commit());
  EXPECT_FALSE(aUpdate.commit());
  // make sure b has won
  LocalTransaction& aCheck = a.beginNewTransaction();
  verify(itemId, 12.34, &aCheck);
}

TEST_F(MultiTransactionSingleCRUTest, InsertInsertCommitDump){
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  insertSample(5.67, &a_insert);
  a_insert.commit();
  LocalTransaction& a_dump = a.beginNewTransaction();
  dump(a_dump);
  EXPECT_EQ(2u, dump_set_.size());
  EXPECT_NE(dump_set_.end(), dump_set_.find(3.14));
  EXPECT_NE(dump_set_.end(), dump_set_.find(5.67));
}

TEST_F(MultiTransactionSingleCRUTest, InsertCommitInsertDump){
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  a_insert.commit();
  LocalTransaction& a_dump = a.beginNewTransaction();
  insertSample(5.67, &a_dump);
  dump(a_dump);
  EXPECT_EQ(2u, dump_set_.size());
  EXPECT_NE(dump_set_.end(), dump_set_.find(3.14));
  EXPECT_NE(dump_set_.end(), dump_set_.find(5.67));
}

TEST_F(MultiTransactionSingleCRUTest, InsertInsertCommitUpdateCommitDump){
  Id to_update;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  to_update = insertSample(5.67, &a_insert);
  a_insert.commit();
  LocalTransaction& a_update = a.beginNewTransaction();
  updateSample(to_update, 9.81, &a_update);
  a_update.commit();
  LocalTransaction& a_dump = a.beginNewTransaction();
  dump(a_dump);
  EXPECT_EQ(2u, dump_set_.size());
  EXPECT_NE(dump_set_.end(), dump_set_.find(3.14));
  EXPECT_NE(dump_set_.end(), dump_set_.find(9.81));
}

TEST_F(MultiTransactionSingleCRUTest, InsertInsertCommitUpdateDump){
  Id to_update;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  to_update = insertSample(5.67, &a_insert);
  a_insert.commit();
  LocalTransaction& a_dump = a.beginNewTransaction();
  updateSample(to_update, 9.81, &a_dump);
  dump(a_dump);
  EXPECT_EQ(2u, dump_set_.size());
  EXPECT_NE(dump_set_.end(), dump_set_.find(3.14));
  EXPECT_NE(dump_set_.end(), dump_set_.find(9.81));
}

TEST_F(MultiTransactionSingleCRUTest, InsertCommitInsertUpdateDump){
  Id to_update;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  a_insert.commit();
  LocalTransaction& a_dump = a.beginNewTransaction();
  to_update = insertSample(5.67, &a_dump);
  updateSample(to_update, 9.81, &a_dump);
  dump(a_dump);
  EXPECT_EQ(2u, dump_set_.size());
  EXPECT_NE(dump_set_.end(), dump_set_.find(3.14));
  EXPECT_NE(dump_set_.end(), dump_set_.find(9.81));
}

TEST_F(MultiTransactionSingleCRUTest, InsertCommitFindUnique){
  Id expected_find;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  insertSample(5.67, &a_insert);
  expected_find = insertSample(9.81, &a_insert);
  a_insert.commit();
  LocalTransaction& a_find = a.beginNewTransaction();
  std::shared_ptr<Revision> found =
      a_find.findUnique(TransactionTestTable::sampleField(), 9.81, table_);
  EXPECT_TRUE(static_cast<bool>(found));
  Id found_id;
  found->get(CRTable::kIdField, &found_id);
  EXPECT_EQ(expected_find, found_id);
}

TEST_F(MultiTransactionSingleCRUTest, InsertCommitInsertFindUnique){
  Id expected_find;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  insertSample(5.67, &a_insert);
  a_insert.commit();
  LocalTransaction& a_find = a.beginNewTransaction();
  expected_find = insertSample(9.81, &a_find);
  std::shared_ptr<Revision> found =
      a_find.findUnique(TransactionTestTable::sampleField(), 9.81, table_);
  EXPECT_TRUE(static_cast<bool>(found));
  Id found_id;
  found->get(CRTable::kIdField, &found_id);
  EXPECT_EQ(expected_find, found_id);
}

TEST_F(MultiTransactionSingleCRUTest, InsertCommitFindNotQuiteUnique){
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  insertSample(5.67, &a_insert);
  insertSample(5.67, &a_insert);
  a_insert.commit();
  LocalTransaction& a_find = a.beginNewTransaction();
  EXPECT_DEATH(a_find.findUnique(
      TransactionTestTable::sampleField(), 5.67, table_), "^");
}

// InsertCommitInsertFindNotQuiteUnique test fails for now, see implementation
// documentation of Transaction::findUnique

TEST_F(MultiTransactionSingleCRUTest, FindMultiCommitetd){
  std::set<Id> expected_find;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  expected_find.insert(insertSample(5.67, &a_insert));
  expected_find.insert(insertSample(5.67, &a_insert));
  a_insert.commit();

  LocalTransaction& a_find = a.beginNewTransaction();
  std::unordered_map<Id, std::shared_ptr<Revision> > found;
  EXPECT_EQ(2u, a_find.find(
      TransactionTestTable::sampleField(), 5.67, table_, &found));
  for (const Id& expected : expected_find) {
    EXPECT_NE(found.end(), found.find(expected));
  }
}

TEST_F(MultiTransactionSingleCRUTest, FindMultiMixed){
  std::set<Id> expected_find;
  Agent& a = addAgent();
  LocalTransaction& a_insert = a.beginNewTransaction();
  insertSample(3.14, &a_insert);
  expected_find.insert(insertSample(5.67, &a_insert));
  a_insert.commit();

  LocalTransaction& a_find = a.beginNewTransaction();
  expected_find.insert(insertSample(5.67, &a_find));
  std::unordered_map<Id, std::shared_ptr<Revision> > found;
  EXPECT_EQ(
      expected_find.size(),
      a_find.find(TransactionTestTable::sampleField(), 5.67, table_, &found));
  for (const Id& expected : expected_find) {
    EXPECT_NE(found.end(), found.find(expected));
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
