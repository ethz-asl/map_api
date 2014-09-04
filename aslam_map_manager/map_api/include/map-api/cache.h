#ifndef MAP_API_CACHE_H_
#define MAP_API_CACHE_H_

#include <unordered_set>

#include <multiagent_mapping_common/mapped-container-base.h>
#include <multiagent_mapping_common/traits.h>

#include "map-api/cache-base.h"
#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/transaction.h"
#include "map-api/unique-id.h"

namespace map_api {
namespace traits {
template <bool IsSharedPointer, typename T>
struct InstanceFactory {
  static T getNewInstance() { return T(); }
  static T* getPointerTo(T& value) { return &value; }  // NOLINT
};
template <typename Type>
struct InstanceFactory<true, Type> {
  static Type getNewInstance() { return Type(new typename Type::element_type); }
  static typename Type::element_type* getPointerTo(Type& value) {  // NOLINT
    return value.get();
  }
};
}  // namespace traits

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
class Cache : public CacheBase,
              public common::MappedContainerBase<IdType, Value> {
 public:
  Cache(const std::shared_ptr<Transaction>& transaction, NetTable* const table,
        const std::shared_ptr<ChunkManagerBase>& chunk_manager);
  virtual ~Cache();
  Value& get(const IdType& id);
  const Value& get(const IdType& id) const;
  /**
   * Inserted objects will live in cache_, but not in revisions_.
   * @return false if some item with same id already exists (in current chunks)
   */
  bool insert(const IdType& id, const Value& value);

  /**
   * Erase object from cache and database.
   */
  void erase(const IdType& id);

  /**
   * Will cache revision of object. TODO(tcies) NetTable::has?
   */
  bool has(const IdType& id) const;
  /**
   * Available with the currently active set of chunks.
   * For now, revisions will be cached. TODO(tcies) method NetTable::dumpIds?
   */
  void getAllAvailableIds(std::unordered_set<IdType>* available_ids) const;

  size_t size() const;
  bool empty() const;

 private:
  static constexpr bool kIsPointer = common::IsPointerType<Value>::value;
  typedef traits::InstanceFactory<kIsPointer, Value> Factory;

  std::shared_ptr<Revision> getRevision(const IdType& id);
  std::shared_ptr<Revision> getRevision(const IdType& id) const;
  virtual void prepareForCommit() override;

  typedef std::unordered_map<IdType, Value> CacheMap;
  typedef std::unordered_set<IdType> IdSet;
  mutable CacheMap cache_;
  mutable CRTable::RevisionMap revisions_;
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
