/*
 * hash_test.cpp
 *
 *  Created on: Mar 31, 2014
 *      Author: titus
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/hash.h"

TEST(Hash, invalid) {
  map_api::Hash result;
  EXPECT_EQ(result.isValid(), false);
}

TEST(Hash, properLength) {
  map_api::Hash result("test");
  EXPECT_EQ(result.getString().length(), 32);
}

TEST(Hash, rudimentaryDiff) {
  map_api::Hash a("Do I contain punctuation?");
  map_api::Hash b("Do I contain punctuation");
  EXPECT_NE(a.getString(), b.getString());
}
