#include <map-api/cr-table-ram-map.h>

namespace map_api {

CRTableRamMap::~CRTableRamMap() {}

bool CRTableRamMap::initCRDerived() { return true; }

bool CRTableRamMap::insertCRDerived(const LogicalTime& /*time*/,
                                    const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  return patchCRDerived(query);
}

bool CRTableRamMap::bulkInsertCRDerived(const NonConstRevisionMap& query,
                                        const LogicalTime& /*time*/) {
  for (const NonConstRevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  // This transitions ownership of the new objects to the db.
  for (const NonConstRevisionMap::value_type& pair : query) {
    CHECK(data_.emplace(pair.first, pair.second).second);
  }
  return true;
}

bool CRTableRamMap::patchCRDerived(const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  return data_.emplace(query->getId<common::Id>(), query).second;
}

void CRTableRamMap::dumpChunkCRDerived(const common::Id& chunk_id,
                                       const LogicalTime& time,
                                       RevisionMap* dest) const {
  CHECK_NOTNULL(dest)->clear();
  for (const MapType::value_type& pair : data_) {
    if (pair.second->getChunkId() == chunk_id) {
      if (pair.second->getInsertTime() <= time) {
        CHECK(dest->emplace(pair.first, pair.second).second);
      }
    }
  }
}

void CRTableRamMap::findByRevisionCRDerived(
    int key, const Revision& valueHolder, const LogicalTime& time,
    RevisionMap* dest) const {
  CHECK_NOTNULL(dest);
  dest->clear();
  // TODO(tcies) allow optimization by index specification
  // global vs local index: local comes in here, global also allows spatial
  // lookup
  for (const MapType::value_type& pair : data_) {
    if (key < 0 || valueHolder.fieldMatch(*pair.second, key)) {
      if (pair.second->getInsertTime() <= time) {
        CHECK(dest->emplace(pair.first, pair.second).second);
      }
    }
  }
}

std::shared_ptr<const Revision> CRTableRamMap::getByIdCRDerived(
    const common::Id& id, const LogicalTime& time) const {
  MapType::const_iterator found = data_.find(id);
  if (found == data_.end() || found->second->getInsertTime() > time) {
    return std::shared_ptr<Revision>();
  }
  return found->second;
}

void CRTableRamMap::getAvailableIdsCRDerived(
    const LogicalTime& time, std::vector<common::Id>* ids) const {
  CHECK_NOTNULL(ids);
  ids->reserve(data_.size());
  for (const MapType::value_type& pair : data_) {
    if (pair.second->getInsertTime() <= time) {
      ids->emplace_back(pair.first);
    }
  }
}

int CRTableRamMap::countByRevisionCRDerived(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) const {
  int count = 0;
  for (const MapType::value_type& pair : data_) {
    if (key < 0 || valueHolder.fieldMatch(*pair.second, key)) {
      if (pair.second->getInsertTime() <= time) {
        ++count;
      }
    }
  }
  return count;
}

int CRTableRamMap::countByChunkCRDerived(const common::Id& chunk_id,
                                         const LogicalTime& time) const {
  int count = 0;
  for (const MapType::value_type& pair : data_) {
    if (pair.second->getChunkId() == chunk_id) {
      if (pair.second->getInsertTime() <= time) {
        ++count;
      }
    }
  }
  return count;
}

void CRTableRamMap::clearCRDerived() { data_.clear(); }

} /* namespace map_api */
