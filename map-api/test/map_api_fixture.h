#ifndef MAP_API_MAP_API_FIXTURE_H_
#define MAP_API_MAP_API_FIXTURE_H_

#include <map_api_test_suite/multiprocess_fixture.h>

class MapApiFixture : public map_api_test_suite::MultiprocessTest {
 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

#endif  // MAP_API_MAP_API_FIXTURE_H_
