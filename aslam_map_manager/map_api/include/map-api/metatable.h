/*
 * metatable.h
 *
 *  Created on: Apr 28, 2014
 *      Author: titus
 */

#ifndef METATABLE_H_
#define METATABLE_H_

#include "map-api/cr-table-interface.h"

namespace map_api {

/**
 * The Metatable is a CR (create and read) table that holds the definitions of
 * all application-defined tables. It is used to synchronize table definitions
 * accross the peers
 */
class Metatable : public CRTableInterface {
 public:
  Metatable(const Id& owner);
  ~Metatable();
  virtual bool init();
  virtual bool define();
 private:
  /**
   * Overriding sync to do nothing - we don't want an infinite recursion
   */
  virtual bool sync();
};

} /* namespace map_api */

#endif /* METATABLE_H_ */
