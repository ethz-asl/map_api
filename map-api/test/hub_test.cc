// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/core.h"
#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "./map_api_fixture.h"

namespace map_api {

class HubTest : public MapApiFixture {};

TEST_F(HubTest, LaunchTest) {
  enum Processes {
    ROOT,
    SLAVE
  };
  enum Barriers {
    BEFORE_COUNT,
    AFTER_COUNT
  };
  if (getSubprocessId() == ROOT) {
    EXPECT_EQ(0, Hub::instance().peerSize());
    launchSubprocess(SLAVE);
    IPC::barrier(BEFORE_COUNT, 1);
    EXPECT_EQ(1, Hub::instance().peerSize());
    IPC::barrier(AFTER_COUNT, 1);
  } else {
    IPC::barrier(BEFORE_COUNT, 1);
    IPC::barrier(AFTER_COUNT, 1);
  }
}

}  // namespace map_api

DMAP_UNITTEST_ENTRYPOINT
