#ifndef MAP_API_CACHE_INL_H_
#define MAP_API_CACHE_INL_H_

#include <glog/logging.h>

namespace map_api {

template <typename IdType, typename Value>
Cache<IdType, Value>::Cache(const std::shared_ptr<Transaction>& transaction,
                            NetTable* table)
    : transaction_(transaction), underlying_table_(CHECK_NOTNULL(table)) {}

template <typename IdType, typename Value>
std::shared_ptr<Value> Cache<IdType, Value>::get(const UniqueId<IdType>& id) {}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::insert(const UniqueId<IdType>& id,
                                  const std::shared_ptr<Value>& value) {}

template <typename IdType, typename Value>
bool Cache<IdType, Value>::has(const UniqueId<IdType>& id) {}

template <typename IdType, typename Value>
void Cache<IdType, Value>::getAllAvailableIds(
    std::unordered_set<IdType>* available_ids) {}

}  // namespace map_api

#endif  // MAP_API_CACHE_INL_H_
