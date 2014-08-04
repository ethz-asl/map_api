#include <map_api_test_suite/multiprocess_fixture.h>

class MultiprocessTest : public map_api_test_suite::MultiprocessTest {
 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};
