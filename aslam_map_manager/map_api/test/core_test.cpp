#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/map-api-core.h>

using namespace map_api;

TEST(MapApiCore, validInit) {
  FLAGS_ip_port = "127.0.0.1:5050";
  MapApiCore &instance = MapApiCore::getInstance();
  EXPECT_TRUE(instance.isInitialized());
  instance.kill();
  EXPECT_FALSE(instance.isInitialized());
}

TEST(MapApiCore, invalidInit) {
  FLAGS_ip_port = "Not an IP-port string";
  EXPECT_DEATH(MapApiCore::getInstance(),"^");
}
