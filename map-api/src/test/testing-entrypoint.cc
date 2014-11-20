#include "map-api/test/testing-entrypoint.h"

#include <fstream>  // NOLINT

#include <gflags/gflags.h>

#include "map-api/file-discovery.h"

DECLARE_uint64(subprocess_id);

namespace map_api {

MapApiTestEntryPoint::~MapApiTestEntryPoint() {}

void MapApiTestEntryPoint::customInit() {
  if (FLAGS_subprocess_id == 0) {
    std::ofstream truncator(FileDiscovery::kFileName,
                            std::ofstream::out | std::ofstream::trunc);
  }
}

}  // namespace map_api
