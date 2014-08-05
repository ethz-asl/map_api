#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map_api_test_suite/multiprocess_fixture.h>

DEFINE_uint64(subprocess_id, 0, "Identification of subprocess in case of "
              "multiprocess testing. 0 if master process.");

namespace map_api_test_suite {
// adapted from
// http://stackoverflow.com/questions/5525668/how-to-implement-readlink-to-find-the-path
std::string getSelfpath() {
  char buff[1024];
  ssize_t len = ::readlink("/proc/self/exe", buff, sizeof(buff) - 1);
  if (len != -1) {
    buff[len] = '\0';
    return std::string(buff);
  } else {
    LOG(FATAL)<< "Failed to read link of /proc/self/exe";
    return "";
  }
}

/**
 * Return own ID: 0 if master
 */
uint64_t MultiprocessTest::getSubprocessId() {
  return FLAGS_subprocess_id;
}
/**
 * Launches a subprocess with given ID. ID can be any positive integer.
 * Dies if ID already used
 */
void MultiprocessTest::launchSubprocess(uint64_t id) {
  launchSubprocess(id, "");
}
void MultiprocessTest::launchSubprocess(uint64_t id,
                                        const std::string& extra_flags) {
  CHECK_NE(0u, id)<< "ID 0 reserved for root process";
  CHECK(subprocesses_.find(id) == subprocesses_.end());
  std::ostringstream command_ss;
  command_ss << getSelfpath() << " ";
  command_ss << "--subprocess_id=" << id << " ";
  // the subprocess must launch only the current test
  const ::testing::TestInfo* const test_info =
  ::testing::UnitTest::GetInstance()->current_test_info();
  command_ss << "--gtest_filter=" << test_info->test_case_name() << "." <<
  test_info->name() << " ";
  command_ss << extra_flags;
  FILE* subprocess = popen(command_ss.str().c_str(), "r");
  setvbuf(subprocess, NULL, _IOLBF, 1024);
  CHECK(subprocesses_.insert(
          std::make_pair(id, subprocess)).second);
}

/**
 * Gathers results from all subprocesses, forwarding them to stdout and
 * propagating failures.
 */
void MultiprocessTest::harvest(bool verbose) {
  for (const std::pair<uint64_t, FILE*>& id_pipe : subprocesses_) {
    FILE* pipe = id_pipe.second;
    CHECK(pipe);
    const size_t size = 1024;
    char buffer[size];
    while (timedFGetS(buffer, size, pipe) != NULL) {
      if (verbose) {
        std::cout << "Sub " << id_pipe.first << ": " << buffer;
      }
      EXPECT_EQ(NULL, strstr(buffer, "[  FAILED  ]"));
      EXPECT_EQ(NULL, strstr(buffer, "*** Check failure stack trace: ***"));
    }
  }
}

/**
 * Because in some situations in harvesting it occurs that fgets() hangs
 * even if the subprocess is already dead, this is added to ensure the
 * continuation of the test.
 */
char* MultiprocessTest::timedFGetS(char* out_buffer, int size, FILE* stream) {
  char* result;
  std::mutex mutex;
  std::condition_variable cv;
  std::unique_lock < std::mutex > lock(mutex);
  std::thread thread(
                     fGetSThread,
                     out_buffer, size, stream, &mutex, &cv, &result);
  thread.detach();
  if (cv.wait_for(lock, std::chrono::milliseconds(10000)) ==
      std::cv_status::no_timeout) {
    return result;
  } else {
    LOG(FATAL)<< "Seems like fgets hangs, something went awry with " <<
    "subprocess exit!";
    return NULL;
  }
}

void MultiprocessTest::fGetSThread(char* out_buffer,
                                          int size, FILE* stream,
                                          std::mutex* mutex,
                                          std::condition_variable* cv,
                                          char** result) {
  CHECK_NOTNULL(stream);
  CHECK_NOTNULL(mutex);
  CHECK_NOTNULL(cv);
  CHECK_NOTNULL(result);
  std::unique_lock < std::mutex > lock(*mutex);
  *result = fgets(out_buffer, size, stream);
  lock.unlock();
  cv->notify_all();
}

void MultiprocessTest::SetUp() {
  SetUpImpl();
}

void MultiprocessTest::TearDown() {
  TearDownImpl();
  if (getSubprocessId() == 0) {
    harvest(false);
  }
}
}  // map_api_test_suite
