/*
 * history.cc
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#include <map-api/history.h>

namespace map_api {

History::History(const std::string& tableName, const Hash& owner) :
    tableName_(tableName), CRTableInterface(owner) {}

bool History::init(){
  return setup(tableName_ + "_history");
}

bool History::define(){
  addField<Hash>("rowId");
  addField<Hash>("previous");
  addField<Revision>("revision");
  addField<Time>("time");
  return true;
}

std::shared_ptr<Revision> History::prepareForInsert(Revision& revision,
                                                    const Hash& previous){
  std::shared_ptr<Revision> query = getTemplate();
  query->set("rowId", revision.get<Hash>("ID"));
  query->set("previous", previous);
  query->set("revision", revision);
  query->set("time", Time());
  return query;
}

std::shared_ptr<Revision> History::revisionAt(const Hash& id,
                                              const Time& time){
  typedef std::shared_ptr<Revision> RevisionPtr;
  RevisionPtr revisionIterator = rawGetRow(id);
  if (!revisionIterator){
    return RevisionPtr();
  }
  while (revisionIterator->get<Time>("time") > time){
    revisionIterator = rawGetRow(revisionIterator->get<Hash>("previous"));
    if (!revisionIterator){
      return RevisionPtr();
    }
  }
  return std::make_shared<Revision>(
      revisionIterator->get<Revision>("time"));
}

} /* namespace map_api */
