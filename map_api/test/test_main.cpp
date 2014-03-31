#include <fstream>

#include <gtest/gtest.h>
#include <glog/logging.h>

int main(int argc, char **argv){
  // pruning database file to avoid INSERT conflicts
  std::ofstream dbfile("database.db");
  dbfile << std::endl;

  google::InitGoogleLogging(argv[0]);
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
