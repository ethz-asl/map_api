#include "map-api/cr-table-ram-map.h"

namespace map_api {

CRTableRamMap::~CRTableRamMap() {}

bool CRTableRamMap::initCRDerived() { return true; }

bool CRTableRamMap::insertCRDerived(Revision* query) {
  CHECK_NOTNULL(query);
  return patchCRDerived(*query);
}

bool CRTableRamMap::bulkInsertCRDerived(const RevisionMap& query) {
  for (const RevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  for (const RevisionMap::value_type& pair : query) {
    CHECK(data_.insert(pair).second);
  }
  return true;
}

bool CRTableRamMap::patchCRDerived(const Revision& query) {
  Id id;
  query.get(kIdField, &id);
  return data_.insert(std::make_pair(id, query)).second;
}

int CRTableRamMap::findByRevisionCRDerived(const std::string& key,
                                           const Revision& valueHolder,
                                           const LogicalTime& time,
                                           RevisionMap* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  // TODO(tcies) allow optimization by index specification
  // global vs local index: local comes in here, global also allows spatial
  // lookup
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    RevisionMap::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      dest->insert(*found);
    }
  } else {
    if (key == "") {
      dest->insert(data_.begin(), data_.end());
    } else {
      for (const RevisionMap::value_type& pair : data_) {
        if (valueHolder.fieldMatch(pair.second, key)) {
          CHECK(dest->insert(pair));
        }
      }
    }
  }
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

int CRTableRamMap::countByRevisionCRDerived(const std::string& key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) {}

} /* namespace map_api */
