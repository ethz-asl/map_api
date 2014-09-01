#ifndef MAP_API_CACHE_H_
#define MAP_API_CACHE_H_

#include "map-api/revision.h"
#include "map-api/transaction.h"
#include "map-api/unique-id.h"

namespace map_api {

/**
 * Needs to be implemented by applications.
 * TODO(tcies) move to another header?
 */
template <typename ObjectType>
void objectFromRevision(const map_api::Revision& revision, ObjectType* object);
template <typename ObjectType>
void objectToRevision(const ObjectType& vertex, map_api::Revision* revision);

/**
 * IdType needs to be a UniqueId
 */
template <typename IdType, typename Value>
class Cache {
 public:
  Cache(const std::shared_ptr<Transaction>& transaction, NetTable* table);
  std::shared_ptr<Value> get(const UniqueId<IdType>& id);
  /**
   * @return false if some item with same id already in cache
   */
  bool insert(const UniqueId<IdType>& id, const std::shared_ptr<Value>& value);
  bool has(const UniqueId<IdType>& id);
  /**
   * Available with the currently active set of chunks.
   */
  void getAllAvailableIds(std::unordered_set<IdType>* available_ids);

 private:
  std::unordered_map<IdType, std::shared_ptr<Value> > cache_;
  CRTable::RevisionMap revisions_;
  std::shared_ptr<Transaction> transaction_;
  NetTable* underlying_table_;
};

}  // namespace map_api

#include "map-api/cache-inl.h"

#endif  // MAP_API_CACHE_H_
