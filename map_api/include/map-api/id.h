/*
 * id.h
 *
 *  Created on: Apr 29, 2014
 *      Author: titus
 */

#ifndef ID_H_
#define ID_H_

#define sm temp

#include <sm/hash_id.hpp>

#undef sm

namespace map_api{
  typedef temp::HashId Id;
} // namespace map_api

#endif /* ID_H_ */
