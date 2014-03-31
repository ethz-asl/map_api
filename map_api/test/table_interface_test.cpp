/*
 * table_interface_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

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


// TODO(tcies) preprocessor magic to cover all field types?
class StringFieldTestTable : public TestTable{
 public:
  virtual bool init(){
    setup("string_field_test_table");
    return true;
  }
  Hash insert(const std::string &value){
    std::shared_ptr<TableInsertQuery> query = getTemplate();
    (*query)["test_field"]->set_stringvalue(value);
    return insertQuery(*query);
  }
  std::string get(const Hash &id){
    std::shared_ptr<TableInsertQuery> row = getRow(id);
    if (!static_cast<bool>(row)){
      LOG(FATAL) << "Row looked for not found.";
    }
    return (*row)["test_field"]->stringvalue();
  }
 protected:
  virtual bool define(){
    addField("test_field", proto::TableFieldDescriptor_Type_STRING);
    return true;
  }
};

TEST(TableInterface, stringFieldInit){
  StringFieldTestTable table;
  table.init();
  std::shared_ptr<TableInsertQuery> structure = table.templateForward();
  EXPECT_EQ(structure->fieldqueries_size(), 3);
  EXPECT_TRUE(fieldOf((*structure)["test_field"], *structure));
}

TEST(TableInterface, stringFieldCreateRead){
  StringFieldTestTable table;
  table.init();
  Hash createTest = table.insert("Test");
  EXPECT_EQ(table.get(createTest), "Test");
}

TEST(TableInterface, stringFieldUpdateRead){
  // TODO(tcies) implement update check
}

// TODO(simon) any idea on how to elegantly do the last 3 tests for all
// kinds of fields (string, double, int, blob etc...
