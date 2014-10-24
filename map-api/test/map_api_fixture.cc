#include <gtest/gtest.h>

#include "map-api/core.h"

#include "./map_api_fixture.h"

void MapApiFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { map_api::Core::instance()->kill(); }
