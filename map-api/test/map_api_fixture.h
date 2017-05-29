#ifndef DMAP_DMAP_FIXTURE_H_
#define DMAP_DMAP_FIXTURE_H_

#include <multiprocess-gtest/multiprocess-fixture.h>

class MapApiFixture : public map_api_common::MultiprocessFixture {
 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

#include "./map_api_fixture_inl.h"

#endif  // DMAP_DMAP_FIXTURE_H_
