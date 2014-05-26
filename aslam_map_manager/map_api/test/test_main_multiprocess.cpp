#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

int main(int argc, char **argv){
  google::InitGoogleLogging(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
