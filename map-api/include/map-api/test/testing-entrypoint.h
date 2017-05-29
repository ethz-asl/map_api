#ifndef DMAP_TESTING_ENTRYPOINT_H_
#define DMAP_TESTING_ENTRYPOINT_H_

#include <map-api-common/test/testing-entrypoint.h>
// Undefining the multiagent mapping entry point for safety.
#undef MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT

namespace map_api {

class MapApiTestEntryPoint : public map_api_common::UnitTestEntryPointBase {
 public:
  ~MapApiTestEntryPoint();

 private:
  void customInit();
};

}  // namespace map_api

#define DMAP_UNITTEST_ENTRYPOINT            \
  int main(int argc, char** argv) {         \
    map_api::MapApiTestEntryPoint entry_point; \
    return entry_point.run(argc, argv);     \
  }

#include "./testing-entrypoint-inl.h"

#endif  // DMAP_TESTING_ENTRYPOINT_H_
