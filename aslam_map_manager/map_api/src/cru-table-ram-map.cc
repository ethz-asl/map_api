#include "map-api/cru-table-ram-map.h"

namespace map_api {

CRUTableRamMap::~CRUTableRamMap() {}

bool CRUTableRamMap::initCRUDerived() { return true; }

bool CRUTableRamMap::insertCRUDerived(Revision* query) {
  CHECK_NOTNULL(query);
  Id id;
  query->get(kIdField, &id);
  HistoryMap::iterator found = data_.find(id);
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
  HistoryMap::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, History())).first;
  }
  for (History::iterator it = found->second.begin(); it != found->second.end();
       ++it) {
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
    HistoryMap::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      History::const_iterator latest = found->second.latestAt(time);
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
    for (const HistoryMap::value_type& pair : data_) {
      History::const_iterator latest = pair.second.latestAt(time, time_index);
      if (latest != pair.second.cend()) {
        if (key == "" || valueHolder.fieldMatch(*latest, key, field_index)) {
          CHECK(dest->find(pair.first) == dest->end());
          CHECK(dest->insert(std::make_pair(
                                 pair.first,
                                 std::make_shared<Revision>(*latest))).second);
        }
      }
    }
  }
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

void CRUTableRamMap::getAvailableIdsCRDerived(const LogicalTime& time,
                                              std::unordered_set<Id>* ids) {
  CHECK_NOTNULL(ids);
  LogicalTime item_time;
  int time_index = getTemplate()->indexOf(kUpdateTimeField);
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time, time_index);
    if (latest != pair.second.cend()) {
      ids->insert(pair.first);
    }
  }
}

int CRUTableRamMap::countByRevisionCRUDerived(const std::string& key,
                                              const Revision& valueHolder,
                                              const LogicalTime& time) {
  // TODO(tcies) can this be merged with find, e.g. using lambdas?
  int count = 0;
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    HistoryMap::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      History::const_iterator latest = found->second.latestAt(time);
      if (latest != found->second.cend()) {
        return 1;
      }
    }
  } else {
    int field_index, time_index = getTemplate()->indexOf(kUpdateTimeField);
    if (key != "") {
      field_index = getTemplate()->indexOf(key);
    }
    for (const HistoryMap::value_type& pair : data_) {
      History::const_iterator latest = pair.second.latestAt(time, time_index);
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

void CRUTableRamMap::findHistoryByRevisionCRUDerived(
    const std::string& key, const Revision& valueHolder,
    const LogicalTime& time, HistoryMap* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  // copy over history
  if (key == kIdField) {
    Id id;
    CHECK(valueHolder.get(kIdField, &id));
    HistoryMap::const_iterator found = data_.find(id);
    if (found != data_.end()) {
      CHECK(dest->insert(*found).second);
    }
  } else {
    int field_index;
    if (key != "") {
      field_index = getTemplate()->indexOf(key);
    }
    for (const HistoryMap::value_type& pair : data_) {
      // using current state for filter
      if (key == "" ||
          valueHolder.fieldMatch(*pair.second.begin(), key, field_index)) {
        CHECK(dest->insert(pair).second);
      }
    }
  }
  // trim to time
  for (HistoryMap::value_type& pair : *dest) {
    pair.second.remove_if([&time](const Revision& item) {
      LogicalTime item_time;
      item.get(kUpdateTimeField, &item_time);
      return item_time > time;
    });
  }
}

} /* namespace map_api */
