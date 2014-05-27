#include <string>
#include <sstream>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

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
  }
}

class MultiprocessTest : public ::testing::Test {
 protected:
  /**
   * Return own ID: 0 if master
   */
  uint64_t getSubprocessId() {
    return FLAGS_subprocess_id;
  }
  /**
   * Launches a subprocess and returns its (internal, not process-) ID
   */
  uint64_t launchSubprocess() {
    uint64_t id = ++subprocess_count_;
    std::ostringstream command_ss;
    command_ss << getSelfpath() << " ";
    command_ss << "--subprocess_id=" << id << " ";
    // the subprocess must launch only the current test
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    command_ss << "--gtest_filter=" << test_info->test_case_name() << "." <<
        test_info->name() << " ";
    // set non-conflicting port for map api hub
    command_ss << "--ipPort=\"127.0.0.1:505" << id << "\" ";
    subprocesses_.push_back(popen(command_ss.str().c_str(), "r"));
    return id;
  }
  /**
   * Gathers results from subprocess, forwarding them to stdout and
   * propagating failures.
   */
  void collectSubprocess(uint64_t id) {
    FILE* pipe = subprocesses_[id - 1];
    CHECK(pipe);
    while (!feof(pipe)) {
      char buffer[1024];
      char* bufptr = &buffer[0];
      size_t size = 1024;
      getline(&bufptr, &size, pipe);
      std::cout << "Sub " << id << ": " << buffer;
      EXPECT_EQ(NULL, strstr(buffer, "[  FAILED  ]"));
    }
  }
 private:
  uint64_t subprocess_count_ = 0;
  std::vector<FILE*> subprocesses_; // access: id-1 TODO(tcies) refactor
};
