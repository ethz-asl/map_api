#ifndef MAP_API_THREADSAFE_CACHE_H_
#define MAP_API_THREADSAFE_CACHE_H_

#include <string>

#include <multiagent-mapping-common/threadsafe-cache.h>

#include "map-api/cache-base.h"
#include "map-api/net-table-transaction-interface.h"

namespace map_api {

template <typename IdType, typename ObjectType>
class ThreadsafeCache
    : public common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                     ObjectType>,
      public CacheBase {
 public:
  typedef common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                  ObjectType> BaseType;

  virtual ~ThreadsafeCache() {}

 private:
  // Takes ownership of the interface.
  ThreadsafeCache(NetTable* table,
                  NetTableTransactionInterface<IdType>* released_interface)
      : BaseType(CHECK_NOTNULL(released_interface)),
        table_(CHECK_NOTNULL(table)),
        transaction_interface_(released_interface) {}
  friend class Transaction;

  virtual void rawToCacheImpl(const std::shared_ptr<const Revision>& raw,
                              ObjectType* cached) const final override {
    objectFromRevision(*raw, CHECK_NOTNULL(cached));
  }

  virtual void cacheToRawImpl(const ObjectType& cached,
                              std::shared_ptr<const Revision>* raw) const
      final override {
    CHECK_NOTNULL(raw);
    std::shared_ptr<Revision> result = table_->getTemplate();
    objectToRevision(cached, result.get());
    *raw = result;
  }

  virtual std::string underlyingTableName() const final override {
    return table_->name();
  }

  virtual void prepareForCommit() final override { this->flush(); }

  virtual size_t numCachedItems() const final override {
    LOG(FATAL) << "This is bogus.";
  }

  virtual size_t size() const final override { return BaseType::size(); }

  NetTable* const table_;
  std::unique_ptr<NetTableTransactionInterface<IdType>> transaction_interface_;
};

}  // namespace map_api

#endif  // MAP_API_THREADSAFE_CACHE_H_
