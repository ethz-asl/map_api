/*
 * core_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/map-api-core.h>

using namespace map_api;

TEST(MapApiCore, uninitialized) {
  MapApiCore &instance = MapApiCore::getInstance();
  EXPECT_FALSE(instance.isInitialized());
}

TEST(MapApiCore, validInit) {
  MapApiCore &instance = MapApiCore::getInstance();
  instance.init("127.0.0.1:5050");
  EXPECT_TRUE(instance.isInitialized());
  instance.kill();
  EXPECT_FALSE(instance.isInitialized());
}

TEST(MapApiCore, invalidInit) {
  MapApiCore &instance = MapApiCore::getInstance();
  EXPECT_FALSE(instance.init("not an IP:port string"));
  EXPECT_FALSE(instance.isInitialized());
}
