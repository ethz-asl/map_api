#ifndef MAP_API_CACHE_BASE_H_
#define MAP_API_CACHE_BASE_H_
#include <string>

namespace map_api {

/**
 * Allows transactions to register caches without needing to know the
 * types of a templated cache.
 */
class CacheBase {
  friend class Transaction;

 public:
  virtual ~CacheBase();

 private:
  virtual std::string underlyingTableName() const = 0;
  virtual void prepareForCommit() = 0;
  virtual size_t numCachedItems() const = 0;
  virtual size_t size() const = 0;
};

}  // namespace map_api

#endif  // MAP_API_CACHE_BASE_H_
