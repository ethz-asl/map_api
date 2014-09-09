#include "map-api/cr-table-ram-map.h"

namespace map_api {

CRTableRamMap::~CRTableRamMap() {}

bool CRTableRamMap::initCRDerived() { return true; }

bool CRTableRamMap::insertCRDerived(Revision* query) {
  CHECK_NOTNULL(query);
  return patchCRDerived(*query);
}

bool CRTableRamMap::bulkInsertCRDerived(const RevisionMap& query,
                                        const LogicalTime& /*time*/) {
  for (const RevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  for (const RevisionMap::value_type& pair : query) {
    CHECK(data_.insert(std::make_pair(pair.first, *pair.second)).second);
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
  LogicalTime item_time;
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    MapType::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      found->second.get(kInsertTimeField, &item_time);
      if (item_time <= time) {
        dest->insert(std::make_pair(found->first,
                                    std::make_shared<Revision>(found->second)));
      }
    }
  } else {
    for (const MapType::value_type& pair : data_) {
      if (key == "" || valueHolder.fieldMatch(pair.second, key)) {
        pair.second.get(kInsertTimeField, &item_time);
        if (item_time <= time) {
          CHECK(dest->insert(
                          std::make_pair(pair.first, std::make_shared<Revision>(
                                                         pair.second))).second);
        }
      }
    }
  }
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

void CRTableRamMap::getAvailableIdsCRDerived(const LogicalTime& time,
                                             std::unordered_set<Id>* ids) {
  CHECK_NOTNULL(ids);
  LogicalTime item_time;
  ids->rehash(data_.size());
  for (const MapType::value_type& pair : data_) {
    pair.second.get(kInsertTimeField, &item_time);
    if (item_time <= time) {
      ids->insert(pair.first);
    }
  }
}

int CRTableRamMap::countByRevisionCRDerived(const std::string& key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) {
  int count = 0;
  LogicalTime item_time;
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    MapType::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      found->second.get(kInsertTimeField, &item_time);
      if (item_time <= time) {
        return 1;
      }
    }
  } else {
    for (const MapType::value_type& pair : data_) {
      if (key == "" || valueHolder.fieldMatch(pair.second, key)) {
        pair.second.get(kInsertTimeField, &item_time);
        if (item_time <= time) {
          ++count;
        }
      }
    }
  }
  return count;
}

} /* namespace map_api */
