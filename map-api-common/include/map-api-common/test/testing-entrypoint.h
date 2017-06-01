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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

/*
* Copyright (C) 2014 Simon Lynen, ASL, ETH Zurich, Switzerland
* You can contact the author at <slynen at ethz dot ch>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* NOTICE:
* The abstraction of the entry point into class and base class has been done by Titus Cieslewski,
* ASL, ETH Zurich, Switzerland in 2015.
*/
#ifndef MAP_API_COMMON_TESTING_ENTRYPOINT_H_
#define MAP_API_COMMON_TESTING_ENTRYPOINT_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

// Let the Eclipse parser see the macro.
#ifndef TEST
#define TEST(a, b) int Test_##a##_##b()
#endif

#ifndef TEST_F
#define TEST_F(a, b) int Test_##a##_##b()
#endif

#ifndef TEST_P
#define TEST_P(a, b) int Test_##a##_##b()
#endif

#ifndef TYPED_TEST
#define TYPED_TEST(a, b) int Test_##a##_##b()
#endif

#ifndef TYPED_TEST_P
#define TYPED_TEST_P(a, b) int Test_##a##_##b()
#endif

#ifndef TYPED_TEST_CASE
#define TYPED_TEST_CASE(a, b) int Test_##a##_##b()
#endif

#ifndef REGISTER_TYPED_TEST_CASE_P
#define REGISTER_TYPED_TEST_CASE_P(a, ...)  int Test_##a()
#endif

#ifndef INSTANTIATE_TYPED_TEST_CASE_P
#define INSTANTIATE_TYPED_TEST_CASE_P(a, ...) int Test_##a()
#endif

namespace map_api_common {

class UnitTestEntryPointBase {
 public:
  virtual ~UnitTestEntryPointBase() {}
  // This function must be inline to avoid linker errors.
  inline int run(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    FLAGS_alsologtostderr = true;
    FLAGS_colorlogtostderr = true;
    customInit();
    return RUN_ALL_TESTS();
  }

 private:
  virtual void customInit() = 0;
};

class UnitTestEntryPoint : public UnitTestEntryPointBase {
 public:
  virtual ~UnitTestEntryPoint() {}

 private:
  virtual void customInit();
};

}  // namespace map_api_common

#define MAP_API_COMMON_UNITTEST_ENTRYPOINT \
  int main(int argc, char** argv) {            \
    map_api_common::UnitTestEntryPoint entry_point;    \
    return entry_point.run(argc, argv);        \
  }

#endif  // MAP_API_COMMON_TESTING_ENTRYPOINT_H_
