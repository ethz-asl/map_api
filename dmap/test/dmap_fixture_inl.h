#ifndef DMAP_DMAP_FIXTURE_INL_H_
#define DMAP_DMAP_FIXTURE_INL_H_

#include <gtest/gtest.h>

#include <dmap/core.h>

void MapApiFixture::SetUpImpl() {
  dmap::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(dmap::Core::instance() != nullptr);
}

void MapApiFixture::TearDownImpl() { dmap::Core::instance()->kill(); }

#endif  // DMAP_DMAP_FIXTURE_INL_H_
