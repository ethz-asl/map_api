#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/map-api-core.h"

#include "test_chord_index.cpp"
#include "multiprocess_fixture.cpp"

class ChordIndexTest : public MultiprocessTest {};

using namespace map_api;

TEST(ChordIndexTest, create) {
  TestChordIndex::instance().create();
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
