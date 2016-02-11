#include <glog/logging.h>

#include "dmap/table-descriptor.h"

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
      std::shared_ptr<dmap::TableDescriptor> descriptor(
          new dmap::TableDescriptor);
      descriptor->setName("test_table");
      table.init(descriptor);
    }
    return table;
  }
};
