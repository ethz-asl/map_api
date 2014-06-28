#include "map-api/map-api-core.h"
#include "map-api/net-table.h"

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

  const std::string kTableName = "chunk_test_table";
  const std::string kFieldName = "chunk_test_field";
  NetTable* table_;
};

INSTANTIATE_TEST_CASE_P(Default, NetTableTest, ::testing::Values(false, true));
