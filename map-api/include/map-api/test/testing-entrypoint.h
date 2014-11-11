#ifndef MAP_API_TESTING_ENTRYPOINT_H_
#define MAP_API_TESTING_ENTRYPOINT_H_

#include <multiagent-mapping-common/test/testing-entrypoint.h>
// Undefining the multiagent mapping entry point for safety
#undef MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT

namespace map_api {

class MapApiTestEntryPoint : public common::UnitTestEntryPointBase {
 public:
  ~MapApiTestEntryPoint();

 private:
  void customInit();
};

}  // namespace map_api

#define MAP_API_UNITTEST_ENTRYPOINT            \
  int main(int argc, char** argv) {            \
    map_api::MapApiTestEntryPoint entry_point; \
    return entry_point.run(argc, argv);        \
  }

#endif  // MAP_API_TESTING_ENTRYPOINT_H_
