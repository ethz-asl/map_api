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

#ifndef MULTIPROCESS_GTEST_MULTIPROCESS_FIXTURE_H_
#define MULTIPROCESS_GTEST_MULTIPROCESS_FIXTURE_H_

#include <condition_variable>
#include <cstdio>
#include <map>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_uint64(subprocess_id);
DECLARE_bool(multiprocess_verbose);

namespace map_api_common {

class MultiprocessFixture : public ::testing::Test {
 protected:
  // adapted from
  // http://stackoverflow.com/questions/5525668/how-to-implement-readlink-to-find-the-path
  std::string getSelfpath();

  /**
   * Return own ID: 0 if master
   */
  uint64_t getSubprocessId() const;

  /**
   * Launches a subprocess with given ID. ID can be any positive integer.
   * Dies if ID already used
   */
  void launchSubprocess(uint64_t id);
  void launchSubprocess(uint64_t id, const std::string& extra_flags);

  /**
   * Gathers results from subprocesses, forwarding them to stdout if verbose and
   * propagating failures.
   */
  void harvest();
  void harvest(uint64_t subprocess_id);

  /**
   * Because in some situations in harvesting it occurs that fgets() hangs
   * even if the subprocess is already dead, this is added to ensure the
   * continuation of the test.
   */
  char* timedFGetS(char* out_buffer, int size, FILE* stream);
  static void fGetSThread(
      char* out_buffer, int size, FILE* stream, std::mutex* mutex,
      std::condition_variable* cv, char** result);

  virtual void SetUp();
  virtual void TearDown();
  virtual void SetUpImpl() = 0;
  virtual void TearDownImpl() = 0;

 private:
  typedef std::map<uint64_t, FILE*> SubprocessMap;
  SubprocessMap subprocesses_;  // map to maintain ordering
};
}  // namespace map_api_common

#include "./multiprocess-fixture-inl.h"

#endif  // MULTIPROCESS_GTEST_MULTIPROCESS_FIXTURE_H_
