#ifndef MAP_API_MAP_API_FIXTURE_INL_H_
#define MAP_API_MAP_API_FIXTURE_INL_H_

#include <gtest/gtest.h>

#include <map-api/core.h>

void MapApiFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

#endif  // MAP_API_MAP_API_FIXTURE_INL_H_
