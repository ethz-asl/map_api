#include <fstream>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>
#include <Poco/Data/Statement.h>

#include "map-api/cru-table-interface.h"
#include "map-api/id.h"
#include "map-api/time.h"
#include "map-api/transaction.h"

#include "test_table.cpp"

using namespace map_api;

TEST(TableInterFace, initEmpty) {
  TestTable<CRTableInterface> table(Id::random());
  table.init();
  std::shared_ptr<Revision> structure = table.getTemplate();
  ASSERT_TRUE(static_cast<bool>(structure));
  EXPECT_EQ(structure->fieldqueries_size(), 2u);
}

/**
 **********************************************************
 * TEMPLATED TABLE WITH A SINGLE FIELD OF THE TEMPLATE TYPE
 **********************************************************
 */

template <typename FieldType>
class FieldTestTable : public TestTable<CRTableInterface> {
 public:
  FieldTestTable(const Id& owner) : TestTable(owner) {}
  virtual bool init() {
    setup("field_test_table");
    return true;
  }
 protected:
  virtual bool define() {
    addField<FieldType>("test_field");
    return true;
  }
};

template <typename FieldType>
class InsertReadFieldTestTable : public FieldTestTable<FieldType> {
 public:
  InsertReadFieldTestTable(const Id& owner)
      : FieldTestTable<FieldType>(owner) {
  }

  bool insertQuery(const Revision& query) {
    return this->rawInsertQuery(query);
  }
};


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
  std::string sample_data_1() {
    return "Test_string_1";
  }
  std::string sample_data_2() {
    return "Test_string_2";
  }
};
template <>
class FieldTest<double> : public ::testing::Test {
 protected:
  double sample_data_1() {
    return 3.14;
  }
  double sample_data_2() {
    return -3.14;
  }
};
template <>
class FieldTest<int32_t> : public ::testing::Test {
 protected:
  int32_t sample_data_1() {
    return 42;
  }
  int32_t sample_data_2() {
    return -42;
  }
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
  int64_t sample_data_1() {
    return 9223372036854775807;
  }
  int64_t sample_data_2() {
    return -9223372036854775807;
  }
};
template <>
class FieldTest<map_api::Time> : public ::testing::Test {
 protected:
  Time sample_data_1() {
    return Time(9223372036854775807);
  }
  Time sample_data_2() {
    return Time(9223372036854775);
  }
};
template <>
class FieldTest<testBlob> : public ::testing::Test {
 protected:
  testBlob sample_data_1() {
    testBlob field;
    *field.mutable_nametype() = map_api::proto::TableFieldDescriptor();
    field.mutable_nametype()->set_name("A name");
    field.mutable_nametype()->
        set_type(map_api::proto::TableFieldDescriptor_Type_DOUBLE);
    field.set_doublevalue(3);
    return field;
  }
  testBlob sample_data_2() {
    testBlob field;
    *field.mutable_nametype() = map_api::proto::TableFieldDescriptor();
    field.mutable_nametype()->set_name("Another name");
    field.mutable_nametype()->
        set_type(map_api::proto::TableFieldDescriptor_Type_INT32);
    field.set_doublevalue(42);
    return field;
  }
};

template <typename TestedType>
class FieldTestWithoutInit : public FieldTest<TestedType> {
 protected:
  virtual void SetUp() {
    this->table_.reset(new InsertReadFieldTestTable<TestedType>(Id::random()));
  }

  std::shared_ptr<Revision> getTemplate() {
    to_insert_ = this->table_->getTemplate();
    return to_insert_;
  }

  Id fillRevision() {
    getTemplate();
    Id inserted = Id::random();
    to_insert_->set("ID", inserted);
    to_insert_->set("test_field", this->sample_data_1());
    return inserted;
  }

  bool insertRevision() {
    return this->table_->insertQuery(*to_insert_);
  }

  std::shared_ptr<InsertReadFieldTestTable<TestedType> > table_;
  std::shared_ptr<Revision> to_insert_;
};

template <typename TestedType>
class FieldTestWithInit : public FieldTestWithoutInit<TestedType> {
 protected:
  virtual void SetUp() {
    this->table_.reset(new InsertReadFieldTestTable<TestedType>(Id::random()));
    this->table_->init();
  }

  virtual void TearDown() {
    std::ofstream dbfile("database.db", std::ios::trunc);
  }
};

/**
 *************************
 * TYPED TABLE FIELD TESTS
 *************************
 */

typedef ::testing::Types<testBlob, std::string, int32_t, double,
    map_api::Id, int64_t, map_api::Time> MyTypes;
TYPED_TEST_CASE(FieldTestWithoutInit, MyTypes);
TYPED_TEST_CASE(FieldTestWithInit, MyTypes);

TYPED_TEST(FieldTestWithInit, Init) {
  EXPECT_EQ(this->getTemplate()->fieldqueries_size(), 3u);
}

TYPED_TEST(FieldTestWithoutInit, CreateBeforeInit) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(this->fillRevision(),"^");
}

TYPED_TEST(FieldTestWithoutInit, ReadBeforeInit) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(this->table_->rawGetRow(Id::random()), "^");
}

TYPED_TEST(FieldTestWithInit, CreateRead) {
  Id inserted = this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  std::shared_ptr<Revision> rowFromTable = this->table_->rawGetRow(inserted);
  EXPECT_TRUE(static_cast<bool>(rowFromTable));
  TypeParam dataFromTable;
  rowFromTable->get("test_field", &dataFromTable);
  EXPECT_EQ(dataFromTable, this->sample_data_1());
}

TYPED_TEST(FieldTestWithInit, ReadInexistentRow) {
  this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  Id other_id = Id::random();
  EXPECT_FALSE(this->table_->rawGetRow(other_id));
}

TYPED_TEST(FieldTestWithInit, ReadInexistentRowData) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  Id inserted = this->fillRevision();
  EXPECT_TRUE(this->insertRevision());

  std::shared_ptr<Revision> rowFromTable = this->table_->rawGetRow(inserted);
  EXPECT_TRUE(static_cast<bool>(rowFromTable));
  TypeParam dataFromTable;
  EXPECT_DEATH(rowFromTable->get("some_other_field", &dataFromTable), "^");
}

// TODO(tcies) do something with these below
//TYPED_TEST(FieldTest, UpdateBeforeInit){
//  FieldTestTable<TypeParam> table;
//  EXPECT_DEATH(table.update(Hash("Give me any hash"),
//                            this->sample_data_1()),"^");
//}
//
//TYPED_TEST(FieldTest, UpdateRead){
//  FieldTestTable<TypeParam> table;
//  table.init();
//  TypeParam readValue;
//  Hash updateTest = table.insert(this->sample_data_1());
//  EXPECT_TRUE(table.get(updateTest, readValue));
//  EXPECT_EQ(readValue, this->sample_data_1());
//  EXPECT_TRUE(table.update(updateTest, this->sample_data_2()));
//  EXPECT_TRUE(table.get(updateTest, readValue));
//  EXPECT_EQ(readValue, this->sample_data_2());
//  table.cleanup();
//}
