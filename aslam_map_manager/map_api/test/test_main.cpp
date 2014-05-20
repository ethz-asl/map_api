#include <fstream>

#include <gtest/gtest.h>
#include <glog/logging.h>

int main(int argc, char **argv){
  google::InitGoogleLogging(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  std::ofstream db_trunc("database.db", std::ios_base::trunc);
  db_trunc.close();
  return RUN_ALL_TESTS();
}
