#include <map-api/test/map_api_fixture.h>

#include <gtest/gtest.h>

#include <map-api/core.h>

void MapApiFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { map_api::Core::instance()->kill(); }
