#include <cstdio>
#include <fstream>   // NOLINT
#include <iostream>  // NOLINT
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiprocess_gtest/multiprocess_fixture.h>

DEFINE_uint64(subprocess_id, 0, "Identification of subprocess in case of "
              "multiprocess testing. 0 if master process.");

namespace map_api_test_suite {
// adapted from
// http://stackoverflow.com/questions/5525668/how-to-implement-readlink-to-find-the-path
std::string MultiprocessTest::getSelfpath() {
  char buff[1024];
#ifdef __APPLE__
  uint32_t len = sizeof(buff) - 1;
  int status = _NSGetExecutablePath(buff, &len);
  if (status != 0) {
    len = -1;
  }
#elif defined __linux__
  ssize_t len = ::readlink("/proc/self/exe", buff, sizeof(buff) - 1);
#else
  LOG(FATAL) << "getSelfpath() not implemented for this platform.";
#endif
  if (len != -1) {
    buff[len] = '\0';
    return std::string(buff);
  } else {
#ifdef __APPLE__
    LOG(FATAL) << "Failed to get executable path from _NSGetExecutablePath()";
#elif defined __linux__
    LOG(FATAL) << "Failed to read link of /proc/self/exe";
#else
    LOG(FATAL) << "getSelfpath() not implemented for this platform.";
#endif
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
  // forward all flags of the parent. must come before the rest so the rest
  // can overwrite this
  std::vector<google::CommandLineFlagInfo> flags;
  google::GetAllFlags(&flags);
  for (const google::CommandLineFlagInfo& flag : flags) {
    command_ss << "--" << flag.name << "=" << flag.current_value << " ";
  }
  command_ss << "--subprocess_id=" << id << " ";
  // the subprocess must launch only the current test
  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  command_ss << "--gtest_filter=" << test_info->test_case_name() << "."
             << test_info->name() << " ";
  command_ss << extra_flags;
  FILE* subprocess = popen(command_ss.str().c_str(), "r");
  setvbuf(subprocess, NULL, _IOLBF, 1024);
  CHECK(subprocesses_.insert(std::make_pair(id, subprocess)).second);
}

/**
 * Gathers results from all subprocesses, forwarding them to stdout and
 * propagating failures.
 */
void MultiprocessTest::harvest(bool verbose) {
  for (const std::pair<uint64_t, FILE*>& id_pipe : subprocesses_) {
    harvest(id_pipe.first, verbose);
  }
}

void MultiprocessTest::harvest(uint64_t subprocess_id, bool verbose) {
  SubprocessMap::iterator found = subprocesses_.find(subprocess_id);
  CHECK(found != subprocesses_.end());
  FILE* pipe = found->second;
  CHECK(pipe);
  constexpr size_t kSize = 1024;
  char buffer[kSize];
  while (timedFGetS(buffer, kSize, pipe) != NULL) {
    if (verbose) {
      std::cout << "Sub " << found->first << ": " << buffer;
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
  std::thread thread(fGetSThread, out_buffer, size, stream, &mutex, &cv,
                     &result);
  thread.detach();
  if (cv.wait_for(lock, std::chrono::milliseconds(10000)) ==
      std::cv_status::no_timeout) {
    return result;
  } else {
    LOG(FATAL) << "Seems like fgets hangs, something went awry with "
               << "subprocess exit!";
    return NULL;
  }
}

void MultiprocessTest::fGetSThread(char* out_buffer, int size, FILE* stream,
                                   std::mutex* mutex,
                                   std::condition_variable* cv, char** result) {
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
}  // namespace map_api_test_suite
