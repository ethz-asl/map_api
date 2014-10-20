#ifndef MAP_API_CR_TABLE_INL_H_
#define MAP_API_CR_TABLE_INL_H_
#include <sstream>  // NOLINT
#include <string>
#include <utility>
#include <vector>

namespace map_api {

template <typename IdType>
std::shared_ptr<const Revision> CRTable::getById(
    const IdType& id, const LogicalTime& time) const {
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK(id.isValid()) << "Supplied invalid ID";
  Id map_api_id;
  sm::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  return getByIdCRDerived(map_api_id, time);
}

template <typename IdType>
void CRTable::getAvailableIds(const LogicalTime& time,
                              std::vector<IdType>* ids) const {
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK_NOTNULL(ids);
  ids->clear();
  std::vector<Id> map_api_ids;
  getAvailableIdsCRDerived(time, &map_api_ids);
  ids->reserve(map_api_ids.size());
  for (const Id& id : map_api_ids) {
    ids->emplace_back(id.toIdType<IdType>());
  }
}

template <typename RevisionType>
template <typename Derived>
typename CRTable::RevisionMapBase<RevisionType>::iterator
CRTable::RevisionMapBase<RevisionType>::find(
    const UniqueId<Derived>& key) {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename RevisionType>
template <typename Derived>
typename CRTable::RevisionMapBase<RevisionType>::const_iterator
CRTable::RevisionMapBase<RevisionType>::find(
    const UniqueId<Derived>& key) const {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return find(id_key);
}

template <typename RevisionType>
std::pair<typename CRTable::RevisionMapBase<RevisionType>::iterator, bool>
CRTable::RevisionMapBase<RevisionType>::insert(
    const std::shared_ptr<RevisionType>& revision) {
  CHECK_NOTNULL(revision.get());
  return insert(std::make_pair(revision->template getId<Id>(), revision));
}

template <typename RevisionType>
template <typename Derived>
std::pair<typename CRTable::RevisionMapBase<RevisionType>::iterator, bool>
CRTable::RevisionMapBase<RevisionType>::insert(
    const UniqueId<Derived>& key,
    const std::shared_ptr<RevisionType>& revision) {
  Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

template <typename ValueType>
void CRTable::find(int key, const ValueType& value, const LogicalTime& time,
                  RevisionMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  this->findByRevision(key, *valueHolder, time, dest);
}

template <typename ValueType>
int CRTable::count(
    int key, const ValueType& value, const LogicalTime& time) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  CHECK(valueHolder != nullptr);
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->countByRevision(key, *valueHolder, time);
}

template <typename ValueType>
std::shared_ptr<const Revision> CRTable::findUnique(
    int key, const ValueType& value, const LogicalTime& time) const {
  RevisionMap results;
  find(key, value, time, &results);
  int count = results.size();
  if (count > 1) {
    std::stringstream report;
    report << "There seems to be more than one (" << count <<
        ") item with given"\
        " value of " << key << ", table " << descriptor_->name() << std::endl;
    report << "Items found at " << time << " are:" << std::endl;
    for (const RevisionMap::value_type result : results) {
      report << result.second->dumpToString() << std::endl;
    }
    LOG(FATAL) << report.str();
  } else if (count == 0) {
    return std::shared_ptr<Revision>();
  } else {
    return results.begin()->second;
  }
}

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_INL_H_
