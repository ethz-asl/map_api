#include <string>

#include "map-api/core.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"

#include "./map_api_multiprocess_fixture.h"

namespace map_api {

class NetTableTest : public MultiprocessTest,
                     public ::testing::WithParamInterface<bool> {
 public:
  virtual void SetUp() {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<int>(kFieldName);
    NetTableManager::instance().addTable(
        GetParam() ? CRTable::Type::CRU : CRTable::Type::CR, &descriptor);
    table_ = &NetTableManager::instance().getTable(kTableName);
  }

  size_t count() {
    CRTable::RevisionMap results;
    table_->dumpActiveChunksAtCurrentTime(&results);
    return results.size();
  }

  void increment(const Id& id, Chunk* chunk, NetTableTransaction* transaction) {
    CHECK_NOTNULL(chunk);
    CHECK_NOTNULL(transaction);
    CRTable::RevisionMap chunk_dump = transaction->dumpChunk(chunk);
    CRTable::RevisionMap::iterator found = chunk_dump.find(id);
    std::shared_ptr<Revision> to_update = found->second;
    int transient_value;
    to_update->get(kFieldName, &transient_value);
    ++transient_value;
    to_update->set(kFieldName, transient_value);
    transaction->update(to_update);
  }

  void increment(NetTable* table, const Id& id, Chunk* chunk,
                 Transaction* transaction) {
    CHECK_NOTNULL(table);
    CHECK_NOTNULL(chunk);
    CHECK_NOTNULL(transaction);
    CRTable::RevisionMap chunk_dump = transaction->dumpChunk(table, chunk);
    CRTable::RevisionMap::iterator found = chunk_dump.find(id);
    std::shared_ptr<Revision> to_update = found->second;
    int transient_value;
    to_update->get(kFieldName, &transient_value);
    ++transient_value;
    to_update->set(kFieldName, transient_value);
    transaction->update(table, to_update);
  }

  Id insert(int n, Chunk* chunk) {
    Id insert_id;
    generateId(&insert_id);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, insert_id);
    to_insert->set(kFieldName, n);
    EXPECT_TRUE(table_->insert(chunk, to_insert.get()));
    return insert_id;
  }

  Id insert(int n, ChunkTransaction* transaction) {
    Id insert_id;
    generateId(&insert_id);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, insert_id);
    to_insert->set(kFieldName, n);
    transaction->insert(to_insert);
    return insert_id;
  }

  void insert(int n, Id* id, Transaction* transaction) {
    CHECK_NOTNULL(id);
    CHECK_NOTNULL(transaction);
    generateId(id);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, *id);
    to_insert->set(kFieldName, n);
    transaction->insert(table_, chunk_, to_insert);
  }

  void update(int n, const Id& id, Transaction* transaction) {
    CHECK_NOTNULL(transaction);
    std::shared_ptr<Revision> to_update =
        transaction->getById(id, table_, chunk_);
    to_update->set(kFieldName, n);
    transaction->update(table_, to_update);
  }

  static const std::string kTableName;
  static const std::string kFieldName;
  NetTable* table_;
  Chunk* chunk_;           // generic chunk pointer for custom use
  Id chunk_id_, item_id_;  // equally generic
};

const std::string NetTableTest::kTableName = "chunk_test_table";
const std::string NetTableTest::kFieldName = "chunk_test_field";

// Parameter true / false tests CRU / CR tables.
INSTANTIATE_TEST_CASE_P(Default, NetTableTest, ::testing::Values(false, true));

}  // namespace map_api
