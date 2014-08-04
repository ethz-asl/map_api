#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <map_api_test_suite/multiprocess_fixture.h>

#include "map-api/map-api-core.h"
#include "map-api/ipc.h"

#include "map_api_multiprocess_fixture.h"

void MultiprocessTest::SetUpImpl() {
  map_api::MapApiCore::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::MapApiCore::instance() != nullptr);
}

void MultiprocessTest::TearDownImpl() {
  map_api::MapApiCore::instance()->kill();
}
