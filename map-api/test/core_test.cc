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
#include "map-api/test/testing-entrypoint.h"

namespace map_api {

TEST(CoreTest, validInit) {
  Core* instance = Core::instance();
  EXPECT_EQ(nullptr, instance);
  Core::initializeInstance();
  instance = Core::instance();
  EXPECT_NE(nullptr, instance);
  EXPECT_TRUE(instance->isInitialized());
  instance->kill();
  EXPECT_EQ(nullptr, Core::instance());
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
