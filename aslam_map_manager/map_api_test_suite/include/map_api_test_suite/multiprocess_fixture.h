#ifndef MAP_API_TEST_SUITE_MULTIPROCESS_FIXTURE_H_
#define MAP_API_TEST_SUITE_MULTIPROCESS_FIXTURE_H_

#include <condition_variable>
#include <cstdio>
#include <thread>
#include <map>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_uint64(subprocess_id);

// adapted from
// http://stackoverflow.com/questions/5525668/how-to-implement-readlink-to-find-the-path
std::string getSelfpath();

namespace map_api_test_suite {

class MultiprocessTest : public ::testing::Test {
 protected:
  /**
   * Return own ID: 0 if master
   */
  uint64_t getSubprocessId();

  /**
   * Launches a subprocess with given ID. ID can be any positive integer.
   * Dies if ID already used
   */
  void launchSubprocess(uint64_t id);
  void launchSubprocess(uint64_t id, const std::string& extra_flags);

  /**
   * Gathers results from all subprocesses, forwarding them to stdout and
   * propagating failures.
   */
  void harvest(bool verbose = true);

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
  std::map<uint64_t, FILE*> subprocesses_; // map to maintain ordering
};
}  // map_api_test_suite
#endif  // MAP_API_TEST_SUITE_MULTIPROCESS_FIXTURE_H_
