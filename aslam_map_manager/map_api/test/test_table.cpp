#include <map-api/cru-table.h>
#include <glog/logging.h>

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
template <typename TableInterfaceType>
class TestTable : public TableInterfaceType {
 public:
  ~TestTable() {}
  virtual const std::string name() const override {
    return "test_table";
  }
 protected:
  virtual void define(){}

 public:
  using TableInterfaceType::rawInsert;
  using TableInterfaceType::rawGetById;
};
