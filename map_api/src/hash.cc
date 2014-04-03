/*
 * hash.cc
 *
 *  Created on: Mar 24, 2014
 *      Author: titus
 */

#include <map-api/hash.h>

#include <glog/logging.h>
#include <Poco/MD5Engine.h>

namespace map_api {

Hash::Hash() : hexHash_("") {}

Hash::Hash(const char* data, int length){
  init(data,length);
}

Hash::Hash(const std::string& str){
  init(str.c_str(), str.length());
}

void Hash::init(const char* data, int length){
  if (data == NULL){
    LOG(FATAL) << "Null pointer to data to be hashed";
  }
  Poco::MD5Engine hasher;
  hasher.update((void*)data, length);
  hexHash_ = Poco::DigestEngine::digestToHex(hasher.digest());
}

const std::string& Hash::getString() const{
  return hexHash_;
}

bool Hash::isValid() const{
  return hexHash_.size() == 32;
}

Hash Hash::cast(const std::string& hex){
  Hash ret;
  ret.hexHash_ = hex;
  return ret;
}

bool Hash::operator==(const Hash& other) const{
  return hexHash_ == other.hexHash_;
}

} /* namespace map_api */
