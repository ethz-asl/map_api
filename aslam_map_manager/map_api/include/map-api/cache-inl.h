#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <utility>

#include <glog/logging.h>

namespace map_api {

template <typename IdType, typename Value>
Cache<IdType, Value>::Cache(
    const std::shared_ptr<Transaction>& transaction, NetTable* const table,
    const std::shared_ptr<ChunkManagerBase>& chunk_manager)
    : underlying_table_(CHECK_NOTNULL(table)),
      chunk_manager_(chunk_manager),
      staged_(false),
      transaction_(transaction) {
  CHECK_NOTNULL(transaction.get());
  CHECK_NOTNULL(chunk_manager.get());
  transaction_.get()->attachCache(underlying_table_, this);
  // Caching available ids. TODO(tcies) fetch ids only
  revisions_ = transaction_.get()->dumpActiveChunks(underlying_table_);
  for (const CRTable::RevisionMap::value_type& revision : revisions_) {
    // need to fetch id from revision for lack of conversion from map_api Id to
    // unique id.
    IdType id;
    revision.second->get(CRTable::kIdField, &id);
    available_ids_.emplace(id);
  }
}

template <typename IdType, typename Value>
Cache<IdType, Value>::~Cache() {}

template <typename IdType, typename Value>
Value& Cache<IdType, Value>::get(const IdType& id) {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;

    cache_insertion = cache_.emplace(id, Factory::getNewInstance());
    CHECK(cache_insertion.second);
    objectFromRevision(*revision,
                       Factory::getPointerTo(cache_insertion.first->second));
    found = cache_insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value>
const Value& Cache<IdType, Value>::get(const IdType& id) const {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;
    cache_insertion = cache_.emplace(id, Factory::getNewInstance());
    CHECK(cache_insertion.second);
    objectFromRevision(*revision, cache_insertion.first->second.get());
    found = cache_insertion.first;
  }
  return found->second;
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::insert(const IdType& id, const Value& value) {
  typename IdSet::iterator found = available_ids_.find(id);
  if (found != available_ids_.end()) {
    return false;
  }
  CHECK(cache_.emplace(id, value).second);
  CHECK(available_ids_.emplace(id).second);
  return true;
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::erase(const IdType& /*id*/) {
  // TODO(tcies): Implement erase from DB.
  CHECK(false) << "Erase on cache not implemented.";
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::has(const IdType& id) const {
  typename IdSet::const_iterator found = this->available_ids_.find(id);
  return found != available_ids_.end();
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::getAllAvailableIds(
    std::unordered_set<IdType>* available_ids) const {
  CHECK_NOTNULL(available_ids);
  available_ids->clear();
  available_ids->insert(available_ids_.begin(), available_ids_.end());
}

template <typename IdType, typename Value>
size_t Cache<IdType, Value>::size() const {
  return available_ids_.size();
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::empty() const {
  return available_ids_.empty();
}

template <typename IdType, typename Value>
std::shared_ptr<Revision> Cache<IdType, Value>::getRevision(const IdType& id) {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_.get()->getById(id, underlying_table_);
    CHECK(revision);
    std::pair<RevisionIterator, bool> insertion =
        revisions_.insert(id, revision);
    CHECK(insertion.second);
  }
  return found->second;
}

template <typename IdType, typename Value>
std::shared_ptr<Revision> Cache<IdType, Value>::getRevision(const IdType& id)
    const {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_.get()->getById(id, underlying_table_);
    CHECK(revision);
    std::pair<RevisionIterator, bool> insertion =
        revisions_.insert(id, revision);
    CHECK(insertion.second);
  }
  return found->second;
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::prepareForCommit() {
  CHECK(!staged_);
  for (const typename CacheMap::value_type& cached_pair : cache_) {
    CRTable::RevisionMap::iterator corresponding_revision =
        revisions_.find(cached_pair.first);
    if (corresponding_revision == revisions_.end()) {
      // all items that were in the db before must have been gotten through
      // the revision cache, so an item not present in the revision cache must
      // have been inserted newly.
      std::shared_ptr<Revision> insertion = underlying_table_->getTemplate();
      objectToRevision(cached_pair.first, *cached_pair.second, insertion.get());
      transaction_.get()->insert(chunk_manager_.get(), insertion);
    } else {
      // TODO(slynen) could be a place to work the is_specialized magic you
      // talked about yesterday wrt CHECK_EQ, to use == only if requiresUpdate
      // not specialized?
      if (requiresUpdate(*cached_pair.second,
                         *corresponding_revision->second)) {
        objectToRevision(cached_pair.first, *cached_pair.second,
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
