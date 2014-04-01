/*
 * table_interface_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>

#include <map-api/table-interface.h>

using namespace map_api;

class TestTable : public TableInterface{
 public:
  virtual bool init(){
    setup("test_table");
    return true;
  }
  std::shared_ptr<TableInsertQuery> templateForward() const{
    return getTemplate();
  }
 protected:
  virtual bool define(){
    return true;
  }
  std::shared_ptr<Poco::Data::Session> sessionForward(){
    return std::shared_ptr<Poco::Data::Session>(ses_);
  }
};

bool fieldOf(proto::TableField *a, const TableInsertQuery &query){
  for (int i = 0; i < query.fieldqueries_size(); ++i){
    if (a == &query.fieldqueries(i))
      return true;
  }
  return false;
}

TEST(TableInterFace, initEmpty){
  TestTable table;
  table.init();
  std::shared_ptr<TableInsertQuery> structure = table.templateForward();
  ASSERT_TRUE(static_cast<bool>(structure));
  EXPECT_EQ(structure->fieldqueries_size(), 2);
  EXPECT_TRUE(fieldOf((*structure)["ID"], *structure));
  EXPECT_TRUE(fieldOf((*structure)["owner"], *structure));
  EXPECT_DEATH(fieldOf((*structure)["not a field"], *structure),"^");
}

/**
 *********************************************
 * TEMPLATED ACCESS TO TYPE-DEPENDENT PROTOBUF
 *********************************************
 */

// TODO (tcies) this might be useful elsewhere?
template <typename T>
struct TemplatedField{
  static void set(map_api::proto::TableField* field, const T& value);
  const static T& get(map_api::proto::TableField* field);
  static map_api::proto::TableFieldDescriptor_Type protobufEnum();
};
template<>
struct TemplatedField<std::string>{
  static void set(map_api::proto::TableField* field,
                  const std::string& value){
    field->set_stringvalue(value);
  }
  const static std::string& get(map_api::proto::TableField* field){
    return std::string(field->stringvalue());
  }
  static map_api::proto::TableFieldDescriptor_Type protobufEnum(){
    return map_api::proto::TableFieldDescriptor_Type_STRING;
  }
};

/**
 **********************************************************
 * TEMPLATED TABLE WITH A SINGLE FIELD OF THE TEMPLATE TYPE
 **********************************************************
 */

template <typename T>
class FieldTestTable : public TestTable{
 public:
  virtual bool init(){
    setup("field_test_table");
    return true;
  }
  void cleanup(){
    *(sessionForward()) << "DROP TABLE IF EXISTS field_test_table" <<
        Poco::Data::now;
    LOG(INFO) << "Table field_test_table dropped";
  }
  Hash insert(const T &value){
    std::shared_ptr<TableInsertQuery> query = getTemplate();
    TemplatedField<T>::set((*query)["test_field"], value);
    return insertQuery(*query);
  }
  T get(const Hash &id){
    std::shared_ptr<TableInsertQuery> row = getRow(id);
    if (!static_cast<bool>(row)){
      LOG(FATAL) << "Row looked for not found.";
    }
    return TemplatedField<T>::get((*row)["test_field"]);
  }
  bool update(const Hash &id, const T& newValue){
    std::shared_ptr<TableInsertQuery> row = getRow(id);
    if (!static_cast<bool>(row)){
      LOG(FATAL) << "Row looked for not found.";
    }
    TemplatedField<T>::set((*row)["test_field"], newValue);
    return updateQuery(id, *row);
  }
 protected:
  virtual bool define(){
    addField("test_field", TemplatedField<T>::protobufEnum());
    return true;
  }
};

/**
 **************************************
 * FIXTURES FOR TYPED TABLE FIELD TESTS
 **************************************
 */


template <typename T>
class FieldTest : public ::testing::Test{
 protected:
  const T sample_data_1();
  const T sample_data_2();
};

template <>
class FieldTest<std::string> : public ::testing::Test{
 protected:
  const std::string sample_data_1(){
    return "Test string 1";
  }
  const std::string sample_data_2(){
    return "Test string 2";
  }
};

/**
 *************************
 * TYPED TABLE FIELD TESTS
 *************************
 */

typedef ::testing::Types<std::string> MyTypes;
TYPED_TEST_CASE(FieldTest, MyTypes);

TYPED_TEST(FieldTest, Init){
  FieldTestTable<TypeParam> table;
  table.init();
  std::shared_ptr<TableInsertQuery> structure = table.templateForward();
  EXPECT_EQ(structure->fieldqueries_size(), 3);
  EXPECT_TRUE(fieldOf((*structure)["test_field"], *structure));
  table.cleanup();
}

TYPED_TEST(FieldTest, CreateRead){
  FieldTestTable<TypeParam> table;
  table.init();
  Hash createTest = table.insert(this->sample_data_1());
  EXPECT_EQ(table.get(createTest), this->sample_data_1());
  table.cleanup();
}

TYPED_TEST(FieldTest, UpdateRead){
  FieldTestTable<TypeParam> table;
  table.init();
  Hash updateTest = table.insert(this->sample_data_2());
  EXPECT_EQ(table.get(updateTest), this->sample_data_2());
  EXPECT_TRUE(table.update(updateTest, this->sample_data_1()));
  EXPECT_EQ(table.get(updateTest), this->sample_data_1());
  table.cleanup();
}

