#include "map-api/cru-table-ram-map.h"

namespace map_api {

CRUTableRamMap::~CRUTableRamMap() {}

bool CRUTableRamMap::initCRUDerived() { return true; }

bool CRUTableRamMap::insertCRUDerived(Revision* query) {
  CHECK_NOTNULL(query);
  Id id;
  query->get(kIdField, &id);
  HistoryMapType::iterator found = data_.find(id);
  if (found != data_.end()) {
    return false;
  }
  data_[id].push_front(*query);
  return true;
}

bool CRUTableRamMap::bulkInsertCRUDerived(const RevisionMap& query) {
  for (const RevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  for (const RevisionMap::value_type& pair : query) {
    data_[pair.first].push_front(*pair.second);
  }
  return true;
}

bool CRUTableRamMap::patchCRDerived(const Revision& query) {
  Id id;
  LogicalTime time, list_time;
  query.get(kIdField, &id);
  query.get(kUpdateTimeField, &time);
  HistoryMapType::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, HistoryType())).first;
  }
  for (HistoryType::iterator it = found->second.begin();
       it != found->second.end(); ++it) {
    it->get(kUpdateTimeField, &list_time);
    if (list_time < time) {
      found->second.insert(it, query);
      return true;
    }
    LOG(WARNING) << "Patching, not in front!";  // shouldn't usually be the case
  }
  found->second.push_back(query);
  return true;
}

int CRUTableRamMap::findByRevisionCRUDerived(const std::string& key,
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
    HistoryMapType::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      HistoryType::const_iterator latest = found->second.latestAt(time);
      if (latest != found->second.cend()) {
        CHECK(dest->insert(std::make_pair(found->first,
                                          std::make_shared<Revision>(*latest)))
                  .second);
      }
    }
  } else {
    int field_index, time_index = getTemplate()->indexOf(kUpdateTimeField);
    if (key != "") {
      field_index = getTemplate()->indexOf(key);
    }
    for (const HistoryMapType::value_type& pair : data_) {
      HistoryType::const_iterator latest =
          pair.second.latestAt(time, time_index);
      if (latest != pair.second.cend()) {
        if (key == "" || valueHolder.fieldMatch(*latest, key, field_index)) {
          CHECK(dest->insert(std::make_pair(
                                 pair.first,
                                 std::make_shared<Revision>(*latest))).second);
        }
      }
    }
  }
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

int CRUTableRamMap::countByRevisionCRUDerived(const std::string& key,
                                              const Revision& valueHolder,
                                              const LogicalTime& time) {
  // TODO(tcies) can this be merged with find, e.g. using lambdas?
  int count = 0;
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    HistoryMapType::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      HistoryType::const_iterator latest = found->second.latestAt(time);
      if (latest != found->second.cend()) {
        return 1;
      }
    }
  } else {
    int field_index, time_index = getTemplate()->indexOf(kUpdateTimeField);
    if (key != "") {
      field_index = getTemplate()->indexOf(key);
    }
    for (const HistoryMapType::value_type& pair : data_) {
      HistoryType::const_iterator latest =
          pair.second.latestAt(time, time_index);
      if (latest != pair.second.cend()) {
        if (key == "" || valueHolder.fieldMatch(*latest, key, field_index)) {
          ++count;
        }
      }
    }
  }
  return count;
}

bool CRUTableRamMap::insertUpdatedCRUDerived(const Revision& query) {
  return patchCRDerived(query);
}

bool CRUTableRamMap::updateCurrentReferToUpdatedCRUDerived(
    const Id& id, const LogicalTime& current_time,
    const LogicalTime& updated_time) {
  // TODO(tcies) pointless, abolish
  CHECK(false) << "Abolish high-level linking";
  return false;
}

CRUTableRamMap::HistoryType::const_iterator
CRUTableRamMap::HistoryType::latestAt(const LogicalTime& time) const {
  return latestAt(time, cbegin()->indexOf(kUpdateTimeField));
}

CRUTableRamMap::HistoryType::const_iterator
CRUTableRamMap::HistoryType::latestAt(const LogicalTime& time,
                                      int index_guess) const {
  LogicalTime item_time;
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    it->get(kUpdateTimeField, index_guess, &item_time);
    if (item_time < time) {
      return it;
    }
  }
  return cend();
}

} /* namespace map_api */
