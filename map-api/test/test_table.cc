#include <glog/logging.h>

#include "map-api/cru-table.h"

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
template <typename CachedTableType>
class TestTable {
 public:
  static CachedTableType& instance() {
    static CachedTableType table;
    if (!table.isInitialized()) {
      std::unique_ptr<map_api::TableDescriptor> descriptor(
          new map_api::TableDescriptor);
      descriptor->setName("test_table");
      table.init(&descriptor);
    }
    return table;
  }
};
