#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/map-api-core.h"
#include "map-api/ipc.h"

DEFINE_uint64(subprocess_id, 0, "Identification of subprocess in case of "\
              "multiprocess testing. 0 if master process.");

// adapted from
// http://stackoverflow.com/questions/5525668/how-to-implement-readlink-to-find-the-path
std::string getSelfpath() {
  char buff[1024];
  ssize_t len = ::readlink("/proc/self/exe", buff, sizeof(buff)-1);
  if (len != -1) {
    buff[len] = '\0';
    return std::string(buff);
  } else {
    LOG(FATAL) << "Failed to read link of /proc/self/exe";
    return "";
  }
}

class MultiprocessTest : public ::testing::Test, public map_api::CoreTester {
 protected:
  /**
   * Return own ID: 0 if master
   */
  uint64_t getSubprocessId() {
    return FLAGS_subprocess_id;
  }
  /**
   * Launches a subprocess with given ID. ID can be any positive integer.
   * Dies if ID already used
   */
  void launchSubprocess(uint64_t id) {
    CHECK_NE(0u, id) << "ID 0 reserved for root process";
    CHECK(subprocesses_.find(id) == subprocesses_.end());
    std::ostringstream command_ss;
    command_ss << getSelfpath() << " ";
    command_ss << "--subprocess_id=" << id << " ";
    // the subprocess must launch only the current test
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    command_ss << "--gtest_filter=" << test_info->test_case_name() << "." <<
        test_info->name() << " ";
    // set non-conflicting port for map api hub
    command_ss << "--ip_port=\"127.0.0.1:505" << id << "\" ";
    FILE* subprocess = popen(command_ss.str().c_str(), "r");
    setvbuf(subprocess, NULL, _IOLBF, 1024);
    CHECK(subprocesses_.insert(
        std::make_pair(id, subprocess)).second);
  }

  /**
   * Gathers results from all subprocesses, forwarding them to stdout and
   * propagating failures.
   */
  void harvest() {
    for (const std::pair<uint64_t, FILE*>& id_pipe : subprocesses_) {
      FILE* pipe = id_pipe.second;
      CHECK(pipe);
      const size_t size = 1024;
      char buffer[size];
      while (timedFGetS(buffer, size, pipe) != NULL) {
        std::cout << "Sub " << id_pipe.first << ": " << buffer;
        EXPECT_EQ(NULL, strstr(buffer, "[  FAILED  ]"));
      }
    }
  }

  /**
   * Because in some situations in harvesting it occurs that fgets() hangs
   * even if the subprocess is already dead, this is added to ensure the
   * continuation of the test.
   */
  char* timedFGetS(char* out_buffer, int size, FILE* stream) {
    char* result;
    std::timed_mutex mutex;
    mutex.lock();
    std::thread thread(fGetSThread, out_buffer, size, stream, &mutex, &result);
    thread.detach();
    if (mutex.try_lock_for(std::chrono::milliseconds(1000))) {
      return result;
    }
    else {
      LOG(FATAL) << "Seems like fgets hangs, something went awry with " <<
          "subprocess exit!";
      return NULL;
    }
  }
  static void fGetSThread(char* out_buffer, int size, FILE* stream,
                           std::timed_mutex* mutex, char** result) {
    CHECK_NOTNULL(mutex);
    *result = fgets(out_buffer, size, stream);
    mutex->unlock();
  }

  virtual void SetUp() {
    map_api::MapApiCore::instance(); // core init
  }

  virtual void TearDown() {
    if (getSubprocessId() == 0) {
      harvest();
      // explicit kill for core in order to carry nothing over to next test
      map_api::MapApiCore::instance().kill();
    }
  }
 private:
  std::map<uint64_t, FILE*> subprocesses_; // map to maintain ordering
};
