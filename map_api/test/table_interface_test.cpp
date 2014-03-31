/*
 * table_interface_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/table-interface.h"

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

TEST(TableInterFace, initEmpty){
  TestTable table;
  table.init();
  std::shared_ptr<TableInsertQuery> structure = table.templateForward();
  EXPECT_EQ(structure->fieldqueries_size(), 2);
  // expecting no death:
  (*structure)["ID"];
  (*structure)["owner"];
  EXPECT_DEATH((*structure)["garbage"],"^");
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
  // expecting no death:
  (*structure)["test_field"];
}

TEST(TableInterface, stringFieldCreateRead){
  StringFieldTestTable table;
  table.init();
  Hash createTest = table.insert("Test");
  EXPECT_EQ(table.get(createTest), "Test");
}

TEST(TableInterface, stringFieldUpdateRead){
  // TODO(tcies)
}

// TODO(simon) any idea on how to elegantly do the last 3 tests for all
// kinds of fields (string, double, int, blob etc...
