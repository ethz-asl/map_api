#ifndef INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_
#define INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_

#include <string>

#include <dmap-common/breakpoints.h>
#include <dmap-common/threadsafe-cache.h>

#include "dmap/cache-base.h"
#include "dmap/internal/object-and-metadata.h"
#include "dmap/net-table-transaction-interface.h"

namespace dmap {

template <typename IdType, typename ObjectType>
class ThreadsafeObjectAndMetadataCache
    : public dmap_common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                     ObjectAndMetadata<ObjectType>> {
 public:
  typedef dmap_common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                  ObjectAndMetadata<ObjectType>> BaseType;

  virtual ~ThreadsafeObjectAndMetadataCache() {}

 private:
  // Takes ownership of the interface.
  explicit ThreadsafeObjectAndMetadataCache(
      NetTableTransactionInterface<IdType>* interface)
      : BaseType(CHECK_NOTNULL(interface)) {}
  friend class ThreadsafeCache<IdType, ObjectType>;

  virtual void rawToCacheImpl(const std::shared_ptr<const Revision>& raw,
                              ObjectAndMetadata<ObjectType>* cached) const
      final override {
    CHECK(raw);
    CHECK_NOTNULL(cached);
    cached->deserialize(*raw);
    CHECK(cached->metadata);
  }

  virtual void cacheToRawImpl(const ObjectAndMetadata<ObjectType>& cached,
                              std::shared_ptr<const Revision>* raw) const
      final override {
    CHECK_NOTNULL(raw);
    cached.serialize(raw);
  }

  virtual bool shouldUpdateImpl(const std::shared_ptr<const Revision>& original,
                                const std::shared_ptr<const Revision>& updated)
      const {
    CHECK(original);
    CHECK(updated);
    return !original->areAllCustomFieldsEqual(*updated);
  }
};

}  // namespace dmap

#endif  // INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_
