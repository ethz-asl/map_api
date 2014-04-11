/*
 * history.cc
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#include <map-api/history.h>

namespace map_api {

History::History(const CRUTableInterface& table) : table_(table){}

bool History::init(){
  return setup(table_.name() + "_history");
}

bool History::define(){
  addField<Hash>("rowId");
  addField<Hash>("previous");
  addField<Revision>("revision");
  addField<Time>("time");
  return true;
}

Hash History::insert(const Revision& revision, const Hash& previous){
  std::shared_ptr<Revision> query = getTemplate();
  query->set("rowId", revision.get<Hash>("ID"));
  query->set("previous", previous);
  query->set("revision", revision);
  query->set("time", Time());
  return insertQuery(*query);
}

std::shared_ptr<Revision> History::revisionAt(const Hash& id,
                                              const Time& time){
  typedef std::shared_ptr<Revision> RevisionPtr;
  RevisionPtr revisionIterator = getRow(id);
  if (!revisionIterator){
    return RevisionPtr();
  }
  while (revisionIterator->get<Time>("time") > time){
    revisionIterator = getRow(revisionIterator->get<Hash>("previous"));
    if (!revisionIterator){
      return RevisionPtr();
    }
  }
  return std::make_shared<Revision>(
      revisionIterator->get<Revision>("time"));
}

} /* namespace map_api */
