#ifndef MAP_API_THREADSAFE_CACHE_H_
#define MAP_API_THREADSAFE_CACHE_H_

#include <multiagent-mapping-common/threadsafe-cache.h>

#include "map-api/net-table-transaction-interface.h"

namespace map_api {

template <typename IdType, typename ObjectType>
class ThreadsafeCache
    : public common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                     ObjectType> {
 public:
  typedef common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                  ObjectType> BaseType;

  ThreadsafeCache(Transaction* const transaction, NetTable* const table,
                  ChunkManagerBase* const chunk_manager)
      : transaction_interface_(transaction, table, chunk_manager),
        BaseType(&transaction_interface_) {}

 private:
  NetTableTransactionInterface<IdType> transaction_interface_;
};

}  // namespace map_api

#endif  // MAP_API_THREADSAFE_CACHE_H_
