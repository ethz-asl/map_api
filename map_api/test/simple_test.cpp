#include <glog/logging.h>
#include <gtest/gtest.h>

template<typename T>
void AddNumbers(const T& lhs, const T& rhs, T* result) {
  CHECK_NOTNULL(result);
  *result = lhs + rhs;
}

TEST(MapApi, AddNumbers) {
  int result;
  AddNumbers(1, 2, &result);
  EXPECT_EQ(result, 3);
}

TEST(MapApi, AddNumbersNegativeDouble) {
  double result;
  AddNumbers(-1.0, -2.0, &result);
  EXPECT_NEAR(result, -3.0, 1e-20);
}

TEST(MapApi, AddNumbersDeathFromNullPtr) {
  int* result = nullptr;
  EXPECT_DEATH(AddNumbers(1, 2, result), "^");
}
