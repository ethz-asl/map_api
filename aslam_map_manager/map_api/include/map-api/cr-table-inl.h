#ifndef MAP_API_CR_TABLE_INL_H_
#define MAP_API_CR_TABLE_INL_H_

#include <sstream>

namespace map_api{

template<typename ValueType>
int CRTable::find(const std::string& key, const ValueType& value,
                     const LogicalTime& time, RevisionMap* dest) {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key != "") {
    valueHolder->set(key, value);
  }
  return this->findByRevision(key, *valueHolder, time, dest);
}

template<typename ValueType>
std::shared_ptr<Revision> CRTable::findUnique(
    const std::string& key, const ValueType& value, const LogicalTime& time) {
  RevisionMap results;
  int count = find(key, value, time, &results);
  if (count > 1) {
    std::stringstream report;
    report << "There seems to be more than one (" << count <<
        ") item with given"\
        " value of " << key << ", table " << descriptor_->name() << std::endl;
    report << "Items found at " << time << " are:" << std::endl;
    for (const RevisionMap::value_type result : results) {
      report << result.second->DebugString() << std::endl;
    }
    LOG(FATAL) << report.str();
  }
  if (count == 0) {
    return std::shared_ptr<Revision>();
  } else {
    return results.begin()->second;
  }
}

} // namespace map_api

#endif /* MAP_API_CR_TABLE_INL_H_ */
