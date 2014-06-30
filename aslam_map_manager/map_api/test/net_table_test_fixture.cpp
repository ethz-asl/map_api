#include "map-api/map-api-core.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"

#include "multiprocess_fixture.cpp"

using namespace map_api;

class NetTableTest : public MultiprocessTest,
public ::testing::WithParamInterface<bool> {
 protected:
  virtual void SetUp() {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<int>(kFieldName);
    MapApiCore::instance().tableManager().addTable(GetParam(), &descriptor);
    table_ = &MapApiCore::instance().tableManager().getTable(kTableName);
  }

  size_t count() {
    std::unordered_map<Id, std::shared_ptr<Revision> > results;
    table_->dumpCache(Time::now(), &results);
    return results.size();
  }

  void increment(const Id& id, NetTableTransaction* transaction) {
    CHECK_NOTNULL(transaction);
    std::shared_ptr<Revision> to_update = transaction->getById(id);
    int transient_value;
    to_update->get(kFieldName, &transient_value);
    ++transient_value;
    to_update->set(kFieldName, transient_value);
    transaction->update(to_update);
  }

  void increment(NetTable* table, const Id& id, Transaction* transaction) {
    CHECK_NOTNULL(table);
    CHECK_NOTNULL(transaction);
    std::shared_ptr<Revision> to_update = transaction->getById(id, table);
    int transient_value;
    to_update->get(kFieldName, &transient_value);
    ++transient_value;
    to_update->set(kFieldName, transient_value);
    transaction->update(table, to_update);
  }

  Id insert(int n, Chunk* chunk) {
    Id insert_id = Id::generate();
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, insert_id);
    to_insert->set(kFieldName, n);
    EXPECT_TRUE(table_->insert(chunk, to_insert.get()));
    return insert_id;
  }

  Id insert(int n, ChunkTransaction* transaction) {
    Id insert_id = Id::generate();
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, insert_id);
    to_insert->set(kFieldName, n);
    transaction->insert(to_insert);
    return insert_id;
  }

  Id popId() const {
    Id id;
    std::string id_string;
    IPC::pop(&id_string);
    id.fromHexString(id_string);
    return id;
  }

  const std::string kTableName = "chunk_test_table";
  const std::string kFieldName = "chunk_test_field";
  NetTable* table_;
};

INSTANTIATE_TEST_CASE_P(Default, NetTableTest, ::testing::Values(false, true));
