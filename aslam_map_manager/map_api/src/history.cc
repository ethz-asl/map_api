#include <map-api/history.h>

namespace map_api {

const std::string History::kPreviousField = "previous";
const std::string History::kRevisionField = "revision";
const std::string History::kRevisionTimeField = "revision_time";

History::History(const std::string& table_name) : table_name_(table_name) {}

const std::string History::name() const {
  return table_name_ + "_history";
}

void History::define(){
  addField<Id>(kPreviousField);
  addField<Revision>(kRevisionField);
  addField<Time>(kRevisionTimeField);
}

} /* namespace map_api */
