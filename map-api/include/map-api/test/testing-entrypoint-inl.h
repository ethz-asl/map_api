#ifndef MAP_API_TESTING_ENTRYPOINT_INL_H_
#define MAP_API_TESTING_ENTRYPOINT_INL_H_

#include <fstream>  // NOLINT

#include <gflags/gflags.h>

#include <multiprocess-gtest/multiprocess-fixture.h>

#include "map-api/file-discovery.h"

namespace map_api {

MapApiTestEntryPoint::~MapApiTestEntryPoint() {}

void MapApiTestEntryPoint::customInit() {
  if (FLAGS_subprocess_id == 0) {
    std::ofstream truncator(FileDiscovery::kFileName,
                            std::ofstream::out | std::ofstream::trunc);
  }
}
}  // namespace map_api

#endif  // MAP_API_TESTING_ENTRYPOINT_INL_H_
