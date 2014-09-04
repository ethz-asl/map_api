#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <utility>

#include <glog/logging.h>

namespace map_api {

template <typename IdType, typename Value, typename DerivedValue>
Cache<IdType, Value, DerivedValue>::Cache(
    const std::shared_ptr<Transaction>& transaction, NetTable* const table,
    const std::shared_ptr<ChunkManagerBase>& chunk_manager)
    : underlying_table_(CHECK_NOTNULL(table)),
      chunk_manager_(chunk_manager),
      staged_(false),
      transaction_(transaction) {
  CHECK_NOTNULL(transaction.get());
  CHECK_NOTNULL(chunk_manager.get());

  transaction_.get()->attachCache(underlying_table_, this);
  transaction_.get()->getAvailableIds(underlying_table_, &available_ids_);
}

template <typename IdType, typename Value, typename DerivedValue>
Cache<IdType, Value, DerivedValue>::~Cache() {}

template <typename IdType, typename Value, typename DerivedValue>
Value& Cache<IdType, Value, DerivedValue>::get(const IdType& id) {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;

    cache_insertion = cache_.emplace(id, Factory::getNewInstance());
    CHECK(cache_insertion.second);
    objectFromRevision(
        *revision, Factory::getPointerToDerived(cache_insertion.first->second));
    found = cache_insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value, typename DerivedValue>
const Value& Cache<IdType, Value, DerivedValue>::get(const IdType& id) const {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;
    cache_insertion = cache_.emplace(id, Factory::getNewInstance());
    CHECK(cache_insertion.second);
    objectFromRevision(
        *revision, Factory::getPointerToDerived(cache_insertion.first->second));
    found = cache_insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::insert(const IdType& id,
                                                const Value& value) {
  typename IdSet::iterator found = available_ids_.find(id);
  if (found != available_ids_.end()) {
    return false;
  }
  CHECK(cache_.emplace(id, value).second);
  CHECK(available_ids_.emplace(id).second);
  return true;
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::erase(const IdType& id) {
  // TODO(tcies): Implement erase from DB.
  LOG_FIRST_N(ERROR, 1) << "Erase on cache will lead to dangling items in the "
                           "db.";
  cache_.erase(id);
  available_ids_.erase(id);
  Id db_id;
  sm::HashId hash_id;
  id.toHashId(&hash_id);
  db_id.fromHashId(hash_id);
  revisions_.erase(db_id);
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::has(const IdType& id) const {
  typename IdSet::const_iterator found = this->available_ids_.find(id);
  return found != available_ids_.end();
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::getAllAvailableIds(
    std::unordered_set<IdType>* available_ids) const {
  CHECK_NOTNULL(available_ids);
  available_ids->clear();
  available_ids->insert(available_ids_.begin(), available_ids_.end());
}

template <typename IdType, typename Value, typename DerivedValue>
size_t Cache<IdType, Value, DerivedValue>::size() const {
  return available_ids_.size();
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::empty() const {
  return available_ids_.empty();
}

template <typename IdType, typename Value, typename DerivedValue>
std::shared_ptr<Revision> Cache<IdType, Value, DerivedValue>::getRevision(
    const IdType& id) {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_.get()->getById(id, underlying_table_);
    CHECK(revision);
    std::pair<RevisionIterator, bool> insertion =
        revisions_.insert(id, revision);
    CHECK(insertion.second);
    found = insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value, typename DerivedValue>
std::shared_ptr<Revision> Cache<IdType, Value, DerivedValue>::getRevision(
    const IdType& id) const {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_.get()->getById(id, underlying_table_);
    CHECK(revision);
    std::pair<RevisionIterator, bool> insertion =
        revisions_.insert(id, revision);
    CHECK(insertion.second);
    found = insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::prepareForCommit() {
  CHECK(!staged_);
  for (const typename CacheMap::value_type& cached_pair : cache_) {
    CRTable::RevisionMap::iterator corresponding_revision =
        revisions_.find(cached_pair.first);
    if (corresponding_revision == revisions_.end()) {
      // all items that were in the db before must have been gotten through
      // the revision cache, so an item not present in the revision cache must
      // have been inserted newly.
      std::shared_ptr<Revision> insertion = underlying_table_->getTemplate();
      objectToRevision(cached_pair.first,
                       Factory::getReferenceToDerived(cached_pair.second),
                       insertion.get());
      transaction_.get()->insert(chunk_manager_.get(), insertion);
    } else {
      // TODO(slynen) determine if the method requires update is specialized.
      // If not try to use operator==, otherwise emit useful message.
      if (requiresUpdate(Factory::getReferenceToDerived(cached_pair.second),
                         *corresponding_revision->second)) {
        objectToRevision(cached_pair.first,
                         Factory::getReferenceToDerived(cached_pair.second),
                         corresponding_revision->second.get());
        transaction_.get()->update(underlying_table_,
                                   corresponding_revision->second);
      }
    }
  }
  staged_ = true;
}

}  // namespace map_api

#endif  // MAP_API_CACHE_INL_H_
