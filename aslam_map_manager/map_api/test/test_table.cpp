#include <glog/logging.h>

#include "map-api/cru-table.h"

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
template <typename TableInterfaceType>
class TestTable : public TableInterfaceType {
 public:
  virtual const std::string name() const override {
    return "test_table";
  }
  virtual void define(){}
  using TableInterfaceType::rawInsert;
  using TableInterfaceType::rawGetById;
  static TestTable& instance() {
    return map_api::CRTable::meyersInstance<TestTable<TableInterfaceType> >();
  }
 protected:
  friend class CRTable;
  TestTable() = default;
  TestTable(const TestTable&) = delete;
  TestTable& operator=(const TestTable&) = delete;
  virtual ~TestTable() {}
};
