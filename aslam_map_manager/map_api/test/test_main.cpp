#include <fstream>

#include <gtest/gtest.h>
#include <glog/logging.h>

int main(int argc, char **argv){
  google::InitGoogleLogging(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
