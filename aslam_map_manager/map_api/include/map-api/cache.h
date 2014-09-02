#ifndef MAP_API_CACHE_H_
#define MAP_API_CACHE_H_

#include <unordered_set>

#include "map-api/cache-base.h"
#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/transaction.h"
#include "map-api/unique-id.h"

namespace map_api {
class NetTable;

/**
 * Needs to be implemented by applications.
 * TODO(tcies) move to another header?
 */
template <typename ObjectType>
void objectFromRevision(const map_api::Revision& revision, ObjectType* object);
template <typename ObjectType>
void objectToRevision(const ObjectType& vertex, map_api::Revision* revision);
template <typename ObjectType>
bool requiresUpdate(const ObjectType& object,
                    const map_api::Revision& revision);

/**
 * IdType needs to be a UniqueId
 */
template <typename IdType, typename Value>
class Cache : public CacheBase {
 public:
  Cache(const std::shared_ptr<Transaction>& transaction, NetTable* table,
        const std::shared_ptr<ChunkManagerBase>& chunk_manager);
  Value& get(const UniqueId<IdType>& id);
  /**
   * Inserted objects will live in cache_, but not in revisions_.
   * @return false if some item with same id already in cache
   */
  bool insert(const UniqueId<IdType>& id, const std::shared_ptr<Value>& value);
  /**
   * Will cache revision of object. TODO(tcies) NetTable::has?
   */
  bool has(const UniqueId<IdType>& id);
  /**
   * Available with the currently active set of chunks.
   * For now, revisions will be cached. TODO(tcies) method NetTable::dumpIds?
   */
  void getAllAvailableIds(std::unordered_set<IdType>* available_ids);

 private:
  std::shared_ptr<Revision> getRevision(const UniqueId<IdType>& id);
  bool hasRevision(const UniqueId<IdType>& id);
  virtual void prepareForCommit() override;

  typedef std::unordered_map<IdType, std::shared_ptr<Value> > CacheMap;
  CacheMap cache_;
  CRTable::RevisionMap revisions_;
  std::shared_ptr<Transaction> transaction_;
  std::shared_ptr<ChunkManagerBase> chunk_manager_;
  NetTable* underlying_table_;
};

}  // namespace map_api

#include "map-api/cache-inl.h"

#endif  // MAP_API_CACHE_H_
