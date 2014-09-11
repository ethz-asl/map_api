#include "map-api/cr-table-ram-map.h"

namespace map_api {

CRTableRamMap::~CRTableRamMap() {}

bool CRTableRamMap::initCRDerived() { return true; }

bool CRTableRamMap::insertCRDerived(const LogicalTime& /*time*/,
                                    Revision* query) {
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
  return data_.emplace(query.getId(), query).second;
}

int CRTableRamMap::findByRevisionCRDerived(int key, const Revision& valueHolder,
                                           const LogicalTime& time,
                                           RevisionMap* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  // TODO(tcies) allow optimization by index specification
  // global vs local index: local comes in here, global also allows spatial
  // lookup
  for (const MapType::value_type& pair : data_) {
    if (key < 0 || valueHolder.fieldMatch(pair.second, key)) {
      if (pair.second.getInsertTime() <= time) {
        CHECK(
            dest->insert(std::make_pair(pair.first, std::make_shared<Revision>(
                                                        pair.second))).second);
      }
    }
  }
  return dest->size();  // TODO(tcies) returning the count is silly, abolish
}

std::shared_ptr<Revision> CRTableRamMap::getByIdCRDerived(
    const Id& id, const LogicalTime& time) {
  MapType::const_iterator found = data_.find(id);
  if (found == data_.end() || found->second.getInsertTime() > time) {
    return std::shared_ptr<Revision>();
  }
  std::shared_ptr<proto::Revision> proto_revision(
      found->second.underlying_revision_);
  return std::shared_ptr<Revision>(new Revision(proto_revision));
}

void CRTableRamMap::getAvailableIdsCRDerived(const LogicalTime& time,
                                             std::unordered_set<Id>* ids) {
  CHECK_NOTNULL(ids);
  ids->rehash(data_.size());
  for (const MapType::value_type& pair : data_) {
    if (pair.second.getInsertTime() <= time) {
      ids->insert(pair.first);
    }
  }
}

int CRTableRamMap::countByRevisionCRDerived(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) {
  int count = 0;
  for (const MapType::value_type& pair : data_) {
    if (key < 0 || valueHolder.fieldMatch(pair.second, key)) {
      if (pair.second.getInsertTime() <= time) {
        ++count;
      }
    }
  }
  return count;
}

} /* namespace map_api */
