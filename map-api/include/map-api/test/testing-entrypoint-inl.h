#ifndef MAP_API_TESTING_ENTRYPOINT_INL_H_
#define MAP_API_TESTING_ENTRYPOINT_INL_H_

#include <fstream>  // NOLINT

#include <gflags/gflags.h>

#include <multiprocess-gtest/multiprocess-fixture.h>

#include "map-api/file-discovery.h"

namespace map_api {

MapApiTestEntryPoint::~MapApiTestEntryPoint() {}

void MapApiTestEntryPoint::customInit() {}

}  // namespace map_api

#endif  // MAP_API_TESTING_ENTRYPOINT_INL_H_
