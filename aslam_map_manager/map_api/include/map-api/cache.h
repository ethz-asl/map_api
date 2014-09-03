#ifndef MAP_API_CACHE_H_
#define MAP_API_CACHE_H_

#include <unordered_set>

#include "map-api/cache-base.h"
#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/transaction.h"
#include "map-api/unique-id.h"

namespace map_api {
class ChunkManagerBase;
class NetTable;

/**
 * Needs to be implemented by applications.
 */
template <typename ObjectType>
void objectFromRevision(const map_api::Revision& revision, ObjectType* object);
template <typename ObjectType>
void objectToRevision(const ObjectType& object, map_api::Revision* revision);
template <typename ObjectType>
bool requiresUpdate(const ObjectType& object,
                    const map_api::Revision& revision);

template <typename IdType, typename ObjectType>
void objectToRevision(const IdType id, const ObjectType& object,
                      map_api::Revision* revision) {
  CHECK_NOTNULL(revision);
  objectToRevision(object, revision);
  IdType present_id;
  revision->get(CRTable::kIdField, &present_id);
  if (present_id.isValid()) {
    CHECK_EQ(id, present_id);
  } else {
    revision->set(CRTable::kIdField, id);
  }
}

/**
 * IdType needs to be a UniqueId
 */
template <typename IdType, typename Value>
class Cache : public CacheBase {
 public:
  Cache(const std::shared_ptr<Transaction>& transaction, NetTable* const table,
        const std::shared_ptr<ChunkManagerBase>& chunk_manager);
  virtual ~Cache();
  Value& get(const IdType& id);
  /**
   * Inserted objects will live in cache_, but not in revisions_.
   * @return false if some item with same id already exists (in current chunks)
   */
  bool insert(const IdType& id, const std::shared_ptr<Value>& value);
  /**
   * Will cache revision of object. TODO(tcies) NetTable::has?
   */
  bool has(const IdType& id);
  /**
   * Available with the currently active set of chunks.
   * For now, revisions will be cached. TODO(tcies) method NetTable::dumpIds?
   */
  void getAllAvailableIds(std::unordered_set<IdType>* available_ids);

 private:
  std::shared_ptr<Revision> getRevision(const IdType& id);
  virtual void prepareForCommit() override;

  typedef std::unordered_map<IdType, std::shared_ptr<Value> > CacheMap;
  typedef std::unordered_set<IdType> IdSet;
  CacheMap cache_;
  CRTable::RevisionMap revisions_;
  IdSet available_ids_;
  NetTable* underlying_table_;
  std::shared_ptr<ChunkManagerBase> chunk_manager_;
  bool staged_;

  class TransactionAccessFactory {
   public:
    class TransactionAccess {
      friend class TransactionAccessFactory;

     public:
      inline Transaction* operator->() const { return transaction_; }
      inline ~TransactionAccess() {
        transaction_->disableDirectAccessForCache();
      }

     private:
      explicit inline TransactionAccess(Transaction* transaction)
          : transaction_(transaction) {
        transaction_->enableDirectAccessForCache();
      }
      Transaction* transaction_;
    };
    explicit inline TransactionAccessFactory(
        std::shared_ptr<Transaction> transaction)
        : transaction_(transaction) {}
    inline TransactionAccess get() const {
      return TransactionAccess(transaction_.get());
    }

   private:
    std::shared_ptr<Transaction> transaction_;
  };
  TransactionAccessFactory transaction_;
};

}  // namespace map_api

#include "map-api/cache-inl.h"

#endif  // MAP_API_CACHE_H_
