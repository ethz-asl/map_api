#include "map-api/cr-table-stxxl-map.h"

namespace map_api {

CRTableSTXXLMap::~CRTableSTXXLMap() { }

bool CRTableSTXXLMap::initCRDerived() {
  return true;
}

bool CRTableSTXXLMap::insertCRDerived(const LogicalTime& /*time*/,
                                      const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  return patchCRDerived(query);
}

bool CRTableSTXXLMap::bulkInsertCRDerived(const NonConstRevisionMap& query,
                                          const LogicalTime& /*time*/) {
  for (const NonConstRevisionMap::value_type& pair : query) {
    if (data_.find(pair.first) != data_.end()) {
      return false;
    }
  }
  // This transitions ownership of the new objects to the db.
  for (const NonConstRevisionMap::value_type& pair : query) {
    patchCRDerived(pair.second);
  }
  return true;
}

bool CRTableSTXXLMap::patchCRDerived(const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  RevisionInformation revision_information;
  CHECK(revision_store_.storeRevision(*query, &revision_information));
  return data_.emplace(query->getId<Id>(), revision_information).second;
}

void CRTableSTXXLMap::dumpChunkCRDerived(const Id& chunk_id,
                                         const LogicalTime& time,
                                         RevisionMap* dest) const {
  CHECK_NOTNULL(dest)->clear();
  for (const MapType::value_type& pair : data_) {
    std::shared_ptr<const Revision> revision;
    CHECK(revision_store_.retrieveRevision(pair.second, &revision));
    if (revision->getChunkId() == chunk_id) {
      if (revision->getInsertTime() <= time) {
        CHECK(dest->emplace(pair.first, revision).second);
      }
    }
  }
}

void CRTableSTXXLMap::findByRevisionCRDerived(int key,
                                              const Revision& valueHolder,
                                              const LogicalTime& time,
                                              RevisionMap* dest) const {
  CHECK_NOTNULL(dest);
  dest->clear();

  for (const MapType::value_type& pair : data_) {
    std::shared_ptr<const Revision> revision;
    CHECK(revision_store_.retrieveRevision(pair.second, &revision));
    if (key < 0 || valueHolder.fieldMatch(*revision, key)) {
      if (revision->getInsertTime() <= time) {
        CHECK(dest->emplace(pair.first, revision).second);
      }
    }
  }
}

std::shared_ptr<const Revision> CRTableSTXXLMap::getByIdCRDerived(
    const Id& id, const LogicalTime& time) const {
  MapType::const_iterator found = data_.find(id);
  if (found == data_.end() || found->second.insert_time_ > time) {
    return std::shared_ptr<Revision>();
  }
  std::shared_ptr<const Revision> revision;
  CHECK(revision_store_.retrieveRevision(found->second, &revision));
  return revision;
}

void CRTableSTXXLMap::getAvailableIdsCRDerived(
    const LogicalTime& time, std::unordered_set<Id>* ids) const {
  CHECK_NOTNULL(ids);
  ids->rehash(data_.size());
  for (const MapType::value_type& pair : data_) {
    if (pair.second.insert_time_ <= time) {
      ids->insert(pair.first);
    }
  }
}

int CRTableSTXXLMap::countByRevisionCRDerived(int key,
                                              const Revision& valueHolder,
                                              const LogicalTime& time) const {
  int count = 0;
  for (const MapType::value_type& pair : data_) {
    // TODO(slynen): Consider caching the data necessary for the checks.
    std::shared_ptr<const Revision> revision;
    CHECK(revision_store_.retrieveRevision(pair.second, &revision));
    if (key < 0 || valueHolder.fieldMatch(*revision, key)) {
      if (revision->getInsertTime() <= time) {
        ++count;
      }
    }
  }
  return count;
}

int CRTableSTXXLMap::countByChunkCRDerived(const Id& chunk_id,
                                           const LogicalTime& time) const {
  int count = 0;
  for (const MapType::value_type& pair : data_) {
    // TODO(slynen): Consider caching the data necessary for the checks.
    std::shared_ptr<const Revision> revision;
    CHECK(revision_store_.retrieveRevision(pair.second, &revision));
    if (revision->getChunkId() == chunk_id) {
      if (revision->getInsertTime() <= time) {
        ++count;
      }
    }
  }
  return count;
}

}  // namespace map_api
