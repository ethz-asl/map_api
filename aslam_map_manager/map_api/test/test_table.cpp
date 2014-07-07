#include <glog/logging.h>

#include "map-api/cru-table.h"

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
template <typename TableInterfaceType>
class TestTable;

template<>
class TestTable<map_api::CRTable> : public map_api::CRTable {
 public:
  virtual const std::string name() const override {
    return "test_table";
  }
  virtual void defineTestTableFields() {}
  virtual void defineFieldsCRDerived() final override {
    defineTestTableFields();
  }
  using map_api::CRTable::rawInsert;
  using map_api::CRTable::rawGetById;
  MEYERS_SINGLETON_INSTANCE_FUNCTION_DIRECT(TestTable)
 protected:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS_DIRECT(TestTable);
};

template<>
class TestTable<map_api::CRUTable> : public map_api::CRUTable {
 public:
  virtual const std::string name() const override {
    return "test_table";
  }
  virtual void defineTestTableFields() {}
  virtual void defineFieldsCRUDerived() final override {
    defineTestTableFields();
  }
  using map_api::CRUTable::rawInsert;
  using map_api::CRUTable::rawGetById;
  MEYERS_SINGLETON_INSTANCE_FUNCTION_DIRECT(TestTable)
 protected:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS_DIRECT(TestTable);
};
