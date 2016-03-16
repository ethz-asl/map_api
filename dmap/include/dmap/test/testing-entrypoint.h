#ifndef DMAP_TESTING_ENTRYPOINT_H_
#define DMAP_TESTING_ENTRYPOINT_H_

#include <multiagent-mapping-common/test/testing-entrypoint.h>
// Undefining the multiagent mapping entry point for safety.
#undef MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT

namespace dmap {

class MapApiTestEntryPoint : public common::UnitTestEntryPointBase {
 public:
  ~MapApiTestEntryPoint();

 private:
  void customInit();
};

}  // namespace dmap

#define DMAP_UNITTEST_ENTRYPOINT            \
  int main(int argc, char** argv) {         \
    dmap::MapApiTestEntryPoint entry_point; \
    return entry_point.run(argc, argv);     \
  }

#include "./testing-entrypoint-inl.h"

#endif  // DMAP_TESTING_ENTRYPOINT_H_
