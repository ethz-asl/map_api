#ifndef MAP_API_MAP_API_FIXTURE_INL_H_
#define MAP_API_MAP_API_FIXTURE_INL_H_

#include <gtest/gtest.h>

#include <dmap/core.h>

void MapApiFixture::SetUpImpl() {
  dmap::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(dmap::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { dmap::Core::instance()->kill(); }

#endif  // MAP_API_MAP_API_FIXTURE_INL_H_
