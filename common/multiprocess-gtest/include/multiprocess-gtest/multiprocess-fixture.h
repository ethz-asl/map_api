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

namespace map_api_test_suite {

class MultiprocessTest : public ::testing::Test {
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
  void harvest(bool verbose = true);
  void harvest(uint64_t subprocess_id, bool verbose);

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
}  // namespace map_api_test_suite
#endif  // MULTIPROCESS_GTEST_MULTIPROCESS_FIXTURE_H_
