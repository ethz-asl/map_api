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
  EXPECT_FALSE(result.isValid());
}

TEST(Hash, valid) {
  Hash result("test");
  EXPECT_TRUE(result.isValid());
  EXPECT_EQ(result.getString().length(), 32u);
}

TEST(Hash, rudimentaryDiff) {
  Hash a("Do I contain punctuation?");
  Hash b("Do I contain punctuation");
  EXPECT_TRUE(a.isValid());
  EXPECT_TRUE(b.isValid());
  EXPECT_NE(a.getString(), b.getString());
}
