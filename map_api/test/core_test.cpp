/*
 * core_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/map-api-core.h"

using namespace map_api;

TEST(MapApiCore, uninitialized) {
  MapApiCore &instance = MapApiCore::getInstance();
  EXPECT_EQ(instance.isInitialized(), false);
}

TEST(MapApiCore, validInit) {
  MapApiCore &instance = MapApiCore::getInstance();
  instance.init("127.0.0.1:5050");
  EXPECT_EQ(instance.isInitialized(), true);
}

TEST(MapApiCore, invalidInit) {
  MapApiCore &instance = MapApiCore::getInstance();
  EXPECT_DEATH(instance.init("wtf"), "^");
}
