#ifndef MAP_API_CR_TABLE_INL_H_
#define MAP_API_CR_TABLE_INL_H_

#include <string>
#include <sstream>  // NOLINT
#include <utility>

namespace map_api {

template <typename IdType>
std::shared_ptr<Revision> CRTable::getById(const IdType& id,
                                           const LogicalTime& time) {
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK(id.isValid()) << "Supplied invalid ID";
  return findUnique(kIdField, id, time);
}

template <typename IdType>
void CRTable::getAvailableIds(const LogicalTime& time,
                              std::unordered_set<IdType>* ids) {
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK_NOTNULL(ids);
  ids->clear();
  std::unordered_set<Id> map_api_ids;
  sm::HashId hash_id;
  IdType out_id;
  getAvailableIdsCRDerived(time, &map_api_ids);
  for (const Id& id : map_api_ids) {
    id.toHashId(&hash_id);
    out_id.fromHashId(hash_id);
    ids->emplace(out_id);
  }
}

template <typename Derived>
CRTable::RevisionMap::iterator CRTable::RevisionMap::find(
    const UniqueId<Derived>& key) {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename Derived>
CRTable::RevisionMap::const_iterator CRTable::RevisionMap::find(
    const UniqueId<Derived>& key) const {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return find(id_key);
}

template <typename Derived>
std::pair<CRTable::RevisionMap::iterator, bool> CRTable::RevisionMap::insert(
    const UniqueId<Derived>& key, const std::shared_ptr<Revision>& revision) {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

template <typename ValueType>
int CRTable::find(const std::string& key, const ValueType& value,
                  const LogicalTime& time, RevisionMap* dest) {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key != "") {
    valueHolder->set(key, value);
  }
  return this->findByRevision(key, *valueHolder, time, dest);
}

template <typename ValueType>
int CRTable::count(const std::string& key, const ValueType& value,
                   const LogicalTime& time) {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  CHECK(valueHolder != nullptr);
  if (!key.empty()) {
    valueHolder->set(key, value);
  }
  return this->countByRevision(key, *valueHolder, time);
}

template <typename ValueType>
std::shared_ptr<Revision> CRTable::findUnique(const std::string& key,
                                              const ValueType& value,
                                              const LogicalTime& time) {
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

template <typename Derived>
bool CRTable::isIdEqual(const Revision& revision, const UniqueId<Derived>& id) {
  Id revision_id;
  revision.get(kIdField, &revision_id);
  return id == revision_id;
}

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_INL_H_
