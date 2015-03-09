#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <utility>
#include <vector>

#include <glog/logging.h>
#include <timing/timer.h>

namespace map_api {

template <typename IdType, typename Value, typename DerivedValue>
Cache<IdType, Value, DerivedValue>::Cache(
    const std::shared_ptr<Transaction>& transaction, NetTable* const table,
    const std::shared_ptr<ChunkManagerBase>& chunk_manager)
    : underlying_table_(CHECK_NOTNULL(table)),
      chunk_manager_(chunk_manager),
      staged_(false),
      transaction_(transaction),
      available_ids_(underlying_table_, &transaction_) {
  CHECK_NOTNULL(transaction.get());
  CHECK_NOTNULL(chunk_manager.get());

  transaction_.get()->attachCache(underlying_table_, this);
}

template <typename IdType, typename Value, typename DerivedValue>
Cache<IdType, Value, DerivedValue>::~Cache() {}

template <typename IdType, typename Value, typename DerivedValue>
Value& Cache<IdType, Value, DerivedValue>::get(const IdType& id) {
  LockGuard lock(mutex_);
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<const Revision> revision = getRevisionLocked(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;

    cache_insertion = cache_.emplace(id, ValueHolder(Value(),
        ValueHolder::DirtyState::kClean));
    CHECK(cache_insertion.second);
    std::shared_ptr<typename Factory::ElementType> object =
        objectFromRevision<typename Factory::ElementType>(*revision);
    Factory::transferOwnership(object, &cache_insertion.first->second.value);
    found = cache_insertion.first;
  }
  found->second.dirty = ValueHolder::DirtyState::kDirty;
  return found->second.value;
}

template <typename IdType, typename Value, typename DerivedValue>
const Value& Cache<IdType, Value, DerivedValue>::get(const IdType& id) const {
  LockGuard lock(mutex_);
  typename CacheMap::iterator found = this->cache_.find(id);
  if (found == this->cache_.end()) {
    std::shared_ptr<const Revision> revision = getRevisionLocked(id);
    CHECK(revision);
    std::pair<typename CacheMap::iterator, bool> cache_insertion;
    cache_insertion = cache_.emplace(id, ValueHolder(Value(),
        ValueHolder::DirtyState::kClean));
    CHECK(cache_insertion.second);
    std::shared_ptr<typename Factory::ElementType> object =
        objectFromRevision<typename Factory::ElementType>(*revision);
    Factory::transferOwnership(object, &cache_insertion.first->second.value);
    found = cache_insertion.first;
  }
  return found->second.value;
}

template <typename IdType, typename Value, typename DerivedValue>
std::shared_ptr<const Revision> Cache<IdType, Value, DerivedValue>::getRevision(
    const IdType& id) const {
  LockGuard lock(mutex_);
  return getRevisionLocked(id);
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::insert(const IdType& id,
                                                const Value& value) {
  LockGuard lock(mutex_);
  bool has_item_already = available_ids_.hasId(id);
  if (has_item_already) {
    return false;
  }
  CHECK(cache_.emplace(
      id, ValueHolder(value, ValueHolder::DirtyState::kClean)).second);
  available_ids_.addId(id);
  return true;
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::erase(const IdType& id) {
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  LockGuard lock(mutex_);
  cache_.erase(id);
  available_ids_.removeId(id);
  removals_.emplace(id);
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::has(const IdType& id) const {
  LockGuard lock(mutex_);
  return available_ids_.hasId(id);
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::getAllAvailableIds(
    std::vector<IdType>* available_ids) const {
  LockGuard lock(mutex_);
  CHECK_NOTNULL(available_ids);
  *available_ids = available_ids_.getAllIds();
}

template <typename IdType, typename Value, typename DerivedValue>
size_t Cache<IdType, Value, DerivedValue>::size() const {
  LockGuard lock(mutex_);
  return available_ids_.getAllIds().size();
}

template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::empty() const {
  LockGuard lock(mutex_);
  return available_ids_.getAllIds().empty();
}

template <typename IdType, typename Value, typename DerivedValue>
std::shared_ptr<const Revision>
Cache<IdType, Value, DerivedValue>::getRevisionLocked(const IdType& id) const {
  typedef CRTable::RevisionMap::iterator RevisionIterator;
  RevisionIterator found = revisions_.find(id);
  if (found == revisions_.end()) {
    std::shared_ptr<const Revision> revision =
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
  LockGuard lock(mutex_);
  CHECK(!staged_) << "You cannot commit a transaction more than once.";
  int num_dirty_items = 0;
  int num_checked_items = 0;
  int num_cached_items = 0;
  for (const typename CacheMap::value_type& cached_pair : cache_) {
    CRTable::RevisionMap::iterator corresponding_revision =
        revisions_.find(cached_pair.first);
    if (corresponding_revision == revisions_.end()) {
      // All items that were in the db before must have been gotten through
      // the revision cache, so an item not present in the revision cache must
      // have been inserted newly.
      std::shared_ptr<Revision> insertion = underlying_table_->getTemplate();
      objectToRevision(cached_pair.first,
                       Factory::getReferenceToDerived(cached_pair.second.value),
                       insertion.get());
      transaction_.get()->insert(chunk_manager_.get(), insertion);
    } else {
      // Only verify objects that have been accessed in a read-write way.
      ++num_cached_items;
      if (cached_pair.second.dirty == ValueHolder::DirtyState::kDirty) {
        // Convert the object to the revision and then compare if it has changed.
        std::shared_ptr<map_api::Revision> update_revision =
            corresponding_revision->second->copyForWrite();
        objectToRevision(cached_pair.first,
                         Factory::getReferenceToDerived(
                             cached_pair.second.value),
                         update_revision.get());
        ++num_checked_items;
        if (*update_revision != *corresponding_revision->second) {
          // To handle addition of fields, we check if the objects also differ
          // if we would reserialize the db version.
          std::shared_ptr<typename Factory::ElementType> value =
              objectFromRevision<typename Factory::ElementType>(
                  *corresponding_revision->second);
          std::shared_ptr<map_api::Revision> reserialized_revision =
              corresponding_revision->second->copyForWrite();
          objectToRevision(cached_pair.first,
                           *value, reserialized_revision.get());

          if (*update_revision != *reserialized_revision) {
            transaction_.get()->update(underlying_table_, update_revision);
            ++num_dirty_items;
          }
        }
      }
    }
  }
  VLOG(3) << "Cache commit: " << underlying_table_->name()
          << " (ca:" << num_cached_items << " ck:" << num_checked_items
          << " d:" << num_dirty_items << ")";
  for (const IdType& id : removals_) {
    // Check if the removed object has ever been part of the database.
    if (revisions_.find(id) == revisions_.end()) {
      continue;
    }
    std::shared_ptr<Revision> to_remove = getRevisionLocked(id)->copyForWrite();
    transaction_.get()->remove(underlying_table_, to_remove);
  }
  staged_ = true;
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::AvailableIds::
getAvailableIdsLocked() const {
  if (!ids_fetched_) {
    timing::Timer timer("getAvailableIds");
    transaction_->get()->getAvailableIds(underlying_table_,
                                         &ordered_available_ids_);
    available_ids_.clear();
    available_ids_.insert(ordered_available_ids_.begin(),
                          ordered_available_ids_.end());
    double total_seconds = timer.Stop();
    ids_fetched_ = true;
    VLOG(3) << "Got " << available_ids_.size() << " ids for table "
            << underlying_table_->name() << " in " << total_seconds << "s";
  }
}

template <typename IdType, typename Value, typename DerivedValue>
Cache<IdType, Value, DerivedValue>::AvailableIds::AvailableIds(
    NetTable* underlying_table, TransactionAccessFactory* transaction) :
    ids_fetched_(false), underlying_table_(underlying_table),
    transaction_(transaction) {}

template <typename IdType, typename Value, typename DerivedValue>
const typename Cache<IdType, Value, DerivedValue>::IdVector&
Cache<IdType, Value, DerivedValue>::AvailableIds::getAllIds() const {
  getAvailableIdsLocked();
  return ordered_available_ids_;
}
template <typename IdType, typename Value, typename DerivedValue>
bool Cache<IdType, Value, DerivedValue>::AvailableIds::hasId(
    const IdType& id) const {
  getAvailableIdsLocked();
  return available_ids_.find(id) != available_ids_.end();
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::AvailableIds::addId(const IdType& id) {
  getAvailableIdsLocked();
  CHECK(available_ids_.emplace(id).second);
  ordered_available_ids_.emplace_back(id);
}

template <typename IdType, typename Value, typename DerivedValue>
void Cache<IdType, Value, DerivedValue>::AvailableIds::removeId(
    const IdType& id) {
  getAvailableIdsLocked();
  available_ids_.erase(id);
  typename std::vector<IdType>::iterator it =
      std::find(ordered_available_ids_.begin(),
                ordered_available_ids_.end(), id);
  if (it != ordered_available_ids_.end()) {
    ordered_available_ids_.erase(it);
  }
}

}  // namespace map_api

#endif  // MAP_API_CACHE_INL_H_
