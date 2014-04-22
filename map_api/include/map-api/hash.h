/*
 * hash.h
 *
 *  Created on: Mar 24, 2014
 *      Author: titus
 */

#ifndef HASH_H_
#define HASH_H_

#include <string>

namespace map_api {

class Hash {
 public:
  /**
   * Construct invalid hash
   */
  Hash();
  /**
   * Construct from char* and length
   */
  Hash(const char* data, int length);
  /**
   * Construct from string
   */
  explicit Hash(const std::string& str);
  /**
   * Return Hash as string
   */
  const std::string& getString() const;
  /**
   * Returns whether Hash valid
   * TODO(simon) does this contradict our previous resolution?
   */
  bool isValid() const;
  /**
   * Cast a hash string to a hash FIXME (titus) protobuf extension instead
   */
  static Hash cast(const std::string& hex);

  inline bool operator==(const Hash& other) const;
  inline bool operator<(const Hash& other) const;
  inline bool operator!() const;

  /**
   * Generates a random hash
   */
  static Hash randomHash();

 private:
  /**
   * To be called from both valid constructors
   */
  void init(const char* data, int length);

  std::string hexHash_;
};

} /* namespace map_api */

#include "map-api/hash-inl.h"

#endif /* HASH_H_ */
