#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <utility>

#include <glog/logging.h>

namespace map_api {

template <typename IdType, typename Value>
Cache<IdType, Value>::Cache(
    const std::shared_ptr<Transaction>& transaction, NetTable* table,
    const std::shared_ptr<ChunkManagerBase>& chunk_manager)
    : transaction_(transaction),
      underlying_table_(CHECK_NOTNULL(table)),
      chunk_manager_(chunk_manager) {
  CHECK_NOTNULL(transaction.get());
  CHECK_NOTNULL(chunk_manager.get());
}

template <typename IdType, typename Value>
Value& Cache<IdType, Value>::get(const UniqueId<IdType>& id) {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<Revision> revision = getRevision(id);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;
    cache_insertion = cache_.emplace(id, std::shared_ptr<Value>(new Value));
    CHECK(cache_insertion.second);
    objectFromRevision(*revision.get(), cache_insertion.first->second.get());
    found = cache_insertion.first;
  }
  return *found->second.get();
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::insert(const UniqueId<IdType>& id,
                                  const std::shared_ptr<Value>& value) {
  return cache_.emplace(id, value).second;
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::has(const UniqueId<IdType>& id) {
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found != cache_.end()) {
    return true;
  }
  return hasRevision(id);
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::getAllAvailableIds(
    std::unordered_set<IdType>* available_ids) {
  CHECK_NOTNULL(available_ids);
  available_ids->clear();
  // will replace current contents of revisions_, which is ok: revisions_
  // represents the database state before commit
  revisions_ = transaction_->dumpActiveChunks(underlying_table_);
  for (const std::shared_ptr<const Revision>& revision : revisions_) {
    IdType id;
    revision->get(CRTable::kIdField, &id);
    available_ids->emplace(id);
  }
}

template <typename IdType, typename Value>
std::shared_ptr<Revision> Cache<IdType, Value>::getRevision(
    const UniqueId<IdType>& id) {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<Revision> revision =
        transaction_->getById(id, underlying_table_);
    if (!revision) {
      return revision;
    }
    std::pair<RevisionIterator, bool> insertion =
        revisions_.emplace(id, revision);
    CHECK(insertion.second);
  }
  return found->second;
}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::hasRevision(const UniqueId<IdType>& id) {
  // TODO(tcies) Transaction::has
  return !!getRevision(id);
}

template <typename IdType, typename Value>
void Cache<IdType, Value>::prepareForCommit() {
  for (const typename CacheMap::value_type& cached_pair : cache_) {
    CRTable::RevisionMap::iterator corresponding_revision =
        revisions_.find(cached_pair);
    if (corresponding_revision == revisions_.end()) {
      // all items that were in the db before must have been gotten through
      // the revision cache, so an item not present in the revision cache must
      // have been inserted newly.
      std::shared_ptr<Revision> insertion(new Revision);
      objectToRevision(*cached_pair.second.get(), insertion.get());
      transaction_->insert(chunk_manager_.get(), insertion);
    } else {
      // TODO(slynen) could be a place to work the is_specialized magic you
      // talked about yesterday wrt CHECK_EQ, to use == only if requiresUpdate
      // not specialized?
      if (requiresUpdate(*cached_pair.second.get(),
                         *corresponding_revision->second.get())) {
        objectToRevision(*cached_pair.second.get(),
                         corresponding_revision->second.get());
        transaction_->update(underlying_table_, corresponding_revision->second);
      }
    }
  }
  // TODO(tcies) ensure this op is called only once, and the cache is discarded
  // afterwards
}

}  // namespace map_api

#endif  // MAP_API_CACHE_INL_H_
