/*
 * hash-inl.h
 *
 *  Created on: Apr 17, 2014
 *      Author: titus
 */

#ifndef HASH_INL_H_
#define HASH_INL_H_

namespace map_api{

inline bool Hash::operator==(const Hash& other) const{
  return hexHash_ == other.hexHash_;
}

inline bool Hash::operator<(const Hash& other) const{
  return hexHash_ < other.hexHash_;
}

inline bool Hash::operator!() const{
  return !isValid();
}

} // namespace map_api

#endif /* HASH_INL_H_ */
