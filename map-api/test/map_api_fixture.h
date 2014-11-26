#ifndef MAP_API_MAP_API_FIXTURE_H_
#define MAP_API_MAP_API_FIXTURE_H_

#include <multiprocess-gtest/multiprocess-fixture.h>

class MapApiFixture : public common::MultiprocessFixture {
 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

#include "./map_api_fixture_inl.h"

#endif  // MAP_API_MAP_API_FIXTURE_H_
