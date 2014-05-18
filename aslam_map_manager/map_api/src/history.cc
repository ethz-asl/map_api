#include <map-api/history.h>

namespace map_api {

History::History(const std::string& table_name) : table_name_(table_name) {}

const std::string History::name() const {
  return table_name_ + "_history";
}

void History::define(){
  addField<Id>("previous");
  addField<Revision>("revision");
  addField<Time>("time");
}

// TODO(tcies) remove once not useful any more
//std::shared_ptr<Revision> History::revisionAt(const Id& id,
//                                              const Time& time){
//  typedef std::shared_ptr<Revision> RevisionPtr;
//  RevisionPtr revisionIterator = rawGetRow(id);
//  if (!revisionIterator){
//    LOG(ERROR) << "Couldn't find id " << id << " in history table " << name();
//    return RevisionPtr();
//  }
//  Time revisionTime;
//  revisionIterator->get("time", &revisionTime);
//  while (revisionTime > time){
//    Id previous;
//    revisionIterator->get("previous", &previous);
//    revisionIterator = rawGetRow(previous);
//    if (!revisionIterator){
//      LOG(ERROR) << "Failed to get previous revision " << previous.hexString();
//      return RevisionPtr();
//    }
//    revisionIterator->get("time", &revisionTime);
//  }
//  std::shared_ptr<Revision> returnValue =
//      std::shared_ptr<Revision>(new Revision);
//  revisionIterator->get("revision", returnValue.get());
//  return returnValue;
//}

} /* namespace map_api */
