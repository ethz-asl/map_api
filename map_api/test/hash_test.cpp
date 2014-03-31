/*
 * hash_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/hash.h"
using namespace map_api;

TEST(Hash, invalid) {
  Hash result;
  EXPECT_EQ(result.isValid(), false);
}

TEST(Hash, valid) {
  Hash result("test");
  EXPECT_EQ(result.isValid(), true);
  EXPECT_EQ(result.getString().length(), 32);
}

TEST(Hash, rudimentaryDiff) {
  Hash a("Do I contain punctuation?");
  Hash b("Do I contain punctuation");
  EXPECT_EQ(a.isValid(), true);
  EXPECT_EQ(b.isValid(), true);
  EXPECT_NE(a.getString(), b.getString());
}
