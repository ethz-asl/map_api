#ifndef DMAP_DMAP_FIXTURE_INL_H_
#define DMAP_DMAP_FIXTURE_INL_H_

#include <gtest/gtest.h>

#include <map-api/core.h>

void MapApiFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

#endif  // DMAP_DMAP_FIXTURE_INL_H_
