#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <utility>

#include <glog/logging.h>

namespace map_api {

template <typename IdType, typename Value>
Cache<IdType, Value>::Cache(
    const std::shared_ptr<Transaction>& transaction, NetTable* const table,
    const std::shared_ptr<ChunkManagerBase>& chunk_manager)
    : transaction_(transaction),
      underlying_table_(CHECK_NOTNULL(table)),
      chunk_manager_(chunk_manager),
      staged_(false) {
  CHECK_NOTNULL(transaction.get());
  CHECK_NOTNULL(chunk_manager.get());
  // Caching available ids. TODO(tcies) fetch ids only
  revisions_ = transaction_->dumpActiveChunks(underlying_table_);
  for (const std::shared_ptr<const Revision>& revision : revisions_) {
    // need to fetch id from revision for lack of conversion from map_api Id to
    // unique id.
    IdType id;
    revision->get(CRTable::kIdField, &id);
    available_ids_->emplace(id);
  }
}

template <typename IdType, typename Value>
Cache<IdType, Value>::~Cache() {}

template <typename IdType, typename Value>
Value& Cache<IdType, Value>::get(const UniqueId<IdType>& id) {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;
    cache_insertion = cache_.emplace(id, std::shared_ptr<Value>(new Value));
    CHECK(cache_insertion.second);
    objectFromRevision(*revision, cache_insertion.first->second.get());
    found = cache_insertion.first;
  }
  return *found->second;
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::insert(const UniqueId<IdType>& id,
                                  const std::shared_ptr<Value>& value) {
  typename IdSet::iterator found = available_ids_.find(id);
  if (found != available_ids_.end()) {
    return false;
  }
  CHECK(cache_.emplace(id, value).second);
  CHECK(available_ids_.emplace(id));
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::has(const UniqueId<IdType>& id) {
  typename IdSet::iterator found = this->available_ids_.find(id);
  return found != available_ids_.end();
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::getAllAvailableIds(
    std::unordered_set<IdType>* available_ids) {
  CHECK_NOTNULL(available_ids);
  available_ids->clear();
  available_ids->insert(available_ids_.begin(), available_ids_.end());
}

template <typename IdType, typename Value>
std::shared_ptr<Revision> Cache<IdType, Value>::getRevision(
    const UniqueId<IdType>& id) {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_->getById(id, underlying_table_);
    CHECK(revision);
    std::pair<RevisionIterator, bool> insertion =
        revisions_.emplace(id, revision);
    CHECK(insertion.second);
  }
  return found->second;
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::prepareForCommit() {
  CHECK(!staged_);
  for (const typename CacheMap::value_type& cached_pair : cache_) {
    CRTable::RevisionMap::iterator corresponding_revision =
        revisions_.find(cached_pair);
    if (corresponding_revision == revisions_.end()) {
      // all items that were in the db before must have been gotten through
      // the revision cache, so an item not present in the revision cache must
      // have been inserted newly.
      std::shared_ptr<Revision> insertion(new Revision);
      objectToRevision(*cached_pair.second, insertion.get());
      transaction_->insert(chunk_manager_.get(), insertion);
    } else {
      // TODO(slynen) could be a place to work the is_specialized magic you
      // talked about yesterday wrt CHECK_EQ, to use == only if requiresUpdate
      // not specialized?
      if (requiresUpdate(*cached_pair.second,
                         *corresponding_revision->second)) {
        objectToRevision(*cached_pair.second,
                         corresponding_revision->second.get());
        transaction_->update(underlying_table_, corresponding_revision->second);
      }
    }
  }
  staged_ = true;
}

}  // namespace map_api

#endif  // MAP_API_CACHE_INL_H_
