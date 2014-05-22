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

} /* namespace map_api */
