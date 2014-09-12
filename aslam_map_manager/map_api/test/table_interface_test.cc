#include <string>
#include <type_traits>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>
#include <Poco/Data/Statement.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <timing/timer.h>

#include "map-api/core.h"
#include "map-api/cr-table-ram-map.h"
#include "map-api/cr-table-ram-sqlite.h"
#include "map-api/cru-table-ram-map.h"
#include "map-api/cru-table-ram-sqlite.h"
#include "map-api/logical-time.h"
#include "map-api/unique-id.h"

#include "./test_table.cc"

namespace map_api {

template <typename TableType>
class ExpectedFieldCount {
 public:
  static int get();
};

template <>
int ExpectedFieldCount<CRTableRamSqlite>::get() {
  return 2;
}

template <>
int ExpectedFieldCount<CRTableRamMap>::get() {
  return 2;
}

template <>
int ExpectedFieldCount<CRUTableRamSqlite>::get() {
  return 4;
}

template <>
int ExpectedFieldCount<CRUTableRamMap>::get() {
  return 4;
}

template <typename TableType>
class TableInterfaceTest : public ::testing::Test {
 public:
  virtual void SetUp() final override {
    Core::initializeInstance();
    ASSERT_TRUE(Core::instance() != nullptr);
  }
  virtual void TearDown() final override { Core::instance()->kill(); }
};

// TODO(tcies) implement remove in CRU sqlite and re-enable tests
typedef ::testing::Types<CRTableRamSqlite,
                         //    CRUTableRamSqlite,
                         CRTableRamMap, CRUTableRamMap> TableTypes;
TYPED_TEST_CASE(TableInterfaceTest, TableTypes);

TYPED_TEST(TableInterfaceTest, initEmpty) {
  TestTable<TypeParam>::instance();
  std::shared_ptr<Revision> structure =
      TestTable<TypeParam>::instance().getTemplate();
  ASSERT_TRUE(static_cast<bool>(structure));
  EXPECT_EQ(ExpectedFieldCount<TypeParam>::get(),
            structure->fieldqueries_size());
}

/**
 **********************************************************
 * TEMPLATED TABLE WITH A SINGLE FIELD OF THE TEMPLATE TYPE
 **********************************************************
 */

template <typename _TableType, typename _DataType>
class TableDataTypes {
 public:
  typedef _TableType TableType;
  typedef _DataType DataType;
};

template <typename TableDataType>
class FieldTestTable : public TestTable<typename TableDataType::TableType> {
 public:
  static const std::string kTestField;
  static typename TableDataType::TableType* forge() {
    typename TableDataType::TableType* table =
        new typename TableDataType::TableType;
    std::unique_ptr<map_api::TableDescriptor> descriptor(
        new map_api::TableDescriptor);
    descriptor->setName("field_test_table");
    descriptor->template addField<typename TableDataType::DataType>(kTestField);
    table->init(&descriptor);
    return table;
  }
};

template <typename TableDataType>
const std::string FieldTestTable<TableDataType>::kTestField = "test_field";

/**
 **************************************
 * FIXTURES FOR TYPED TABLE FIELD TESTS
 **************************************
 */
template <typename TestedType>
class FieldTest : public ::testing::Test {
 protected:
  /**
   * Sample data for tests. MUST BE NON-DEFAULT!
   */
  TestedType sample_data_1();
  TestedType sample_data_2();
};

template <>
class FieldTest<std::string> : public ::testing::Test {
 protected:
  std::string sample_data_1() { return "Test_string_1"; }
  std::string sample_data_2() { return "Test_string_2"; }
};
template <>
class FieldTest<double> : public ::testing::Test {
 protected:
  double sample_data_1() { return 3.14; }
  double sample_data_2() { return -3.14; }
};
template <>
class FieldTest<int32_t> : public ::testing::Test {
 protected:
  int32_t sample_data_1() { return 42; }
  int32_t sample_data_2() { return -42; }
};
template <>
class FieldTest<map_api::Id> : public ::testing::Test {
 protected:
  map_api::Id sample_data_1() {
    map_api::Id id;
    id.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    return id;
  }
  map_api::Id sample_data_2() {
    map_api::Id id;
    id.fromHexString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    return id;
  }
};
template <>
class FieldTest<int64_t> : public ::testing::Test {
 protected:
  int64_t sample_data_1() { return 9223372036854775807; }
  int64_t sample_data_2() { return -9223372036854775807; }
};
template <>
class FieldTest<map_api::LogicalTime> : public ::testing::Test {
 protected:
  LogicalTime sample_data_1() { return LogicalTime(9223372036854775807u); }
  LogicalTime sample_data_2() { return LogicalTime(9223372036854775u); }
};
template <>
class FieldTest<testBlob> : public ::testing::Test {
 protected:
  testBlob sample_data_1() {
    testBlob field;
    *field.mutable_nametype() = map_api::proto::TableFieldDescriptor();
    field.mutable_nametype()->set_name("A name");
    field.mutable_nametype()->set_type(
        map_api::proto::TableFieldDescriptor_Type_DOUBLE);
    field.set_doublevalue(3);
    return field;
  }
  testBlob sample_data_2() {
    testBlob field;
    *field.mutable_nametype() = map_api::proto::TableFieldDescriptor();
    field.mutable_nametype()->set_name("Another name");
    field.mutable_nametype()->set_type(
        map_api::proto::TableFieldDescriptor_Type_INT32);
    field.set_doublevalue(42);
    return field;
  }
};

template <typename TableDataType>
class FieldTestWithoutInit
    : public FieldTest<typename TableDataType::DataType> {
 public:
  virtual ~FieldTestWithoutInit() {}

  virtual void SetUp() override {
    FieldTest<typename TableDataType::DataType>::SetUp();
    table_.reset(new typename TableDataType::TableType());
  }

  virtual void TearDown() override {
    FieldTest<typename TableDataType::DataType>::TearDown();
  }

 protected:
  typedef FieldTestTable<TableDataType> FieldTestTableType;
  std::shared_ptr<Revision> getTemplate() {
    query_ = this->table_->getTemplate();
    return query_;
  }

  Id fillRevision(const typename TableDataType::DataType& value) {
    getTemplate();
    Id inserted;
    generateId(&inserted);
    query_->set(CRTable::kIdField, inserted);
    // to_insert_->set("owner", Id::random()); TODO(tcies) later, from core
    query_->set(FieldTestTableType::kTestField, value);
    return inserted;
  }

  Id fillRevision() { return fillRevision(this->sample_data_1()); }

  bool insertRevision() { return this->table_->insert(query_.get()); }

  void getRevision(const Id& id) {
    query_ = this->table_->getById(id, LogicalTime::sample());
  }

  std::unique_ptr<typename TableDataType::TableType> table_;
  std::shared_ptr<Revision> query_;
};

template <typename TableDataType>
class FieldTestWithInit : public FieldTestWithoutInit<TableDataType> {
 public:
  virtual ~FieldTestWithInit() {}

 protected:
  virtual void SetUp() override {
    Core::initializeInstance();
    ASSERT_TRUE(Core::instance() != nullptr);
    this->table_.reset(FieldTestTable<TableDataType>::forge());
  }
  virtual void TearDown() override { Core::instance()->kill(); }
};

template <typename TableDataType>
class UpdateFieldTestWithInit : public FieldTestWithInit<TableDataType> {
 protected:
  bool updateRevision() { return this->table_->update(this->query_.get()); }

  void fillRevisionWithOtherData() {
    this->query_->set("test_field", this->sample_data_2());
  }
};

template <typename TableType>
class IntTestWithInit
    : public FieldTestWithInit<TableDataTypes<TableType, int64_t>> {
};  // NOLINT

// TODO(tcies) extend tests to SQLite
class CruMapIntTestWithInit
    : public UpdateFieldTestWithInit<TableDataTypes<CRUTableRamMap, int64_t>> {
};

/**
 *************************
 * TYPED TABLE FIELD TESTS
 *************************
 */

#define ALL_DATA_TYPES(table_type)                                             \
  TableDataTypes<table_type, testBlob>,                                        \
      TableDataTypes<table_type, std::string>,                                 \
      TableDataTypes<table_type, int32_t>, TableDataTypes<table_type, double>, \
      TableDataTypes<table_type, map_api::Id>,                                 \
      TableDataTypes<table_type, int64_t>,                                     \
      TableDataTypes<table_type, map_api::LogicalTime>

// TODO(tcies) implement remove in CRU sqlite and re-enable tests
typedef ::testing::Types<
    ALL_DATA_TYPES(CRTableRamSqlite), ALL_DATA_TYPES(CRTableRamMap),
    //                         ALL_DATA_TYPES(CRUTableRamSqlite),
    ALL_DATA_TYPES(CRUTableRamMap)> CrAndCruTypes;

typedef ::testing::Types<
    //    ALL_DATA_TYPES(CRUTableRamSqlite),
    ALL_DATA_TYPES(CRUTableRamMap)> CruTypes;

TYPED_TEST_CASE(FieldTestWithoutInit, CrAndCruTypes);
TYPED_TEST_CASE(FieldTestWithInit, CrAndCruTypes);
TYPED_TEST_CASE(UpdateFieldTestWithInit, CruTypes);
TYPED_TEST_CASE(IntTestWithInit, TableTypes);

TYPED_TEST(FieldTestWithInit, Init) {
  EXPECT_EQ(ExpectedFieldCount<typename TypeParam::TableType>::get() + 1,
            this->getTemplate()->fieldqueries_size());
}

TYPED_TEST(FieldTestWithoutInit, CreateBeforeInit) {
  ::testing::FLAGS_gtest_death_test_style = "fast";
  EXPECT_DEATH(this->fillRevision(),
               "Can't get template of non-initialized table");
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
}

TYPED_TEST(FieldTestWithoutInit, ReadBeforeInit) {
  ::testing::FLAGS_gtest_death_test_style = "fast";
  Id item_id;
  generateId(&item_id);
  EXPECT_DEATH(this->table_->getById(item_id, LogicalTime::sample()),
               "Attempted to getById from non-initialized table");
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
}

TYPED_TEST(FieldTestWithInit, CreateRead) {
  Id inserted = this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  std::shared_ptr<Revision> rowFromTable =
      this->table_->getById(inserted, LogicalTime::sample());
  ASSERT_TRUE(static_cast<bool>(rowFromTable));
  typename TypeParam::DataType dataFromTable;
  rowFromTable->get(FieldTestTable<TypeParam>::kTestField, &dataFromTable);
  EXPECT_EQ(this->sample_data_1(), dataFromTable);
}

TYPED_TEST(FieldTestWithInit, ReadInexistentRow) {
  this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  Id other_id;
  generateId(&other_id);
  EXPECT_FALSE(this->table_->getById(other_id, LogicalTime::sample()));
}

TYPED_TEST(FieldTestWithInit, ReadInexistentRowData) {
  Id inserted = this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  std::shared_ptr<Revision> rowFromTable =
      this->table_->getById(inserted, LogicalTime::sample());
  EXPECT_TRUE(static_cast<bool>(rowFromTable));
  typename TypeParam::DataType dataFromTable;
  ::testing::FLAGS_gtest_death_test_style = "fast";
  EXPECT_DEATH(rowFromTable->get("some_other_field", &dataFromTable),
               "Trying to get inexistent field");
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
}

TYPED_TEST(UpdateFieldTestWithInit, UpdateRead) {
  Id inserted = this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  std::shared_ptr<Revision> rowFromTable =
      this->table_->getById(inserted, LogicalTime::sample());
  ASSERT_TRUE(static_cast<bool>(rowFromTable));
  typename TypeParam::DataType dataFromTable;
  rowFromTable->get("test_field", &dataFromTable);
  EXPECT_EQ(this->sample_data_1(), dataFromTable);

  this->fillRevisionWithOtherData();
  this->updateRevision();
  rowFromTable = this->table_->getById(inserted, LogicalTime::sample());
  EXPECT_TRUE(static_cast<bool>(rowFromTable));
  rowFromTable->get("test_field", &dataFromTable);
  EXPECT_EQ(this->sample_data_2(), dataFromTable);
}

TYPED_TEST(IntTestWithInit, CreateReadThousand) {
  for (int i = 0; i < 1000; ++i) {
    Id inserted = this->fillRevision(i);
    timing::Timer insert_timer("insert - " +
                               std::string(::testing::UnitTest::GetInstance()
                                               ->current_test_info()
                                               ->test_case_name()));
    EXPECT_TRUE(this->insertRevision());
    insert_timer.Stop();

    timing::Timer read_timer("read - " +
                             std::string(::testing::UnitTest::GetInstance()
                                             ->current_test_info()
                                             ->test_case_name()));
    std::shared_ptr<Revision> rowFromTable =
        this->table_->getById(inserted, LogicalTime::sample());
    read_timer.Stop();
    ASSERT_TRUE(static_cast<bool>(rowFromTable));
    int64_t dataFromTable;
    rowFromTable->get(
        FieldTestTable<TableDataTypes<TypeParam, int64_t>>::kTestField,
        &dataFromTable);
    EXPECT_EQ(i, dataFromTable);
  }
  LOG(INFO) << timing::Timing::Print();
}

TEST_F(CruMapIntTestWithInit, HistoryAtTime) {
  constexpr int64_t kFirst = 42, kSecond = 21, kThird = 84;
  Id id = fillRevision(kFirst);
  ASSERT_TRUE(insertRevision());
  getRevision(id);
  ASSERT_TRUE(query_.get() != nullptr);
  query_->set(FieldTestTableType::kTestField, kSecond);
  ASSERT_TRUE(updateRevision());
  LogicalTime before_third = LogicalTime::sample();
  getRevision(id);
  ASSERT_TRUE(query_.get() != nullptr);
  query_->set(FieldTestTableType::kTestField, kThird);
  ASSERT_TRUE(updateRevision());

  CRUTable::HistoryMap old_history_map;
  table_->findHistory(CRTable::kIdField, id, before_third, &old_history_map);
  EXPECT_EQ(1, old_history_map.size());
  const CRUTable::History& old_history = old_history_map.begin()->second;
  EXPECT_EQ(2, old_history.size());

  CRUTable::HistoryMap new_history_map;
  table_->findHistory(CRTable::kIdField, id, LogicalTime::sample(),
                      &new_history_map);
  EXPECT_EQ(1, new_history_map.size());
  const CRUTable::History& new_history = new_history_map.begin()->second;
  EXPECT_EQ(3, new_history.size());
}

TEST_F(CruMapIntTestWithInit, Remove) {
  constexpr int64_t kValue = 42;
  fillRevision(kValue);
  insertRevision();

  EXPECT_EQ(1, table_->count("", 0, LogicalTime::sample()));
  std::unordered_set<Id> ids;
  table_->getAvailableIds(LogicalTime::sample(), &ids);
  EXPECT_EQ(1, ids.size());
  CRTable::RevisionMap result;
  table_->find("", 0, LogicalTime::sample(), &result);
  EXPECT_EQ(1, result.size());

  table_->remove(LogicalTime::sample(), result.begin()->second.get());

  EXPECT_EQ(0, table_->count("", 0, LogicalTime::sample()));
  table_->getAvailableIds(LogicalTime::sample(), &ids);
  EXPECT_EQ(0, ids.size());
  table_->find("", 0, LogicalTime::sample(), &result);
  EXPECT_EQ(0, result.size());
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
