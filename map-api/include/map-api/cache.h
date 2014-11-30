#ifndef MAP_API_CACHE_H_
#define MAP_API_CACHE_H_
#include <mutex>
#include <unordered_set>
#include <vector>

#include <multiagent-mapping-common/mapped-container-base.h>
#include <multiagent-mapping-common/traits.h>

#include <map-api/cache-base.h>
#include <map-api/cr-table.h>
#include <map-api/revision.h>
#include <map-api/transaction.h>
#include <map-api/unique-id.h>

namespace map_api {
namespace traits {
template <bool IsSharedPointer, typename Type, typename DerivedType>
struct InstanceFactory {
  typedef DerivedType ElementType;
  static DerivedType* getPointerTo(Type& value) { return &value; }   // NOLINT
  static DerivedType& getReferenceTo(Type& value) { return value; }  // NOLINT
  static const DerivedType& getReferenceTo(const Type& value) {      // NOLINT
    return value;
  }
  static DerivedType* getPointerToDerived(Type& value) {  // NOLINT
    DerivedType* ptr = static_cast<DerivedType*>(&value);
    CHECK_NOTNULL(ptr);
    return ptr;
  }
  static DerivedType& getReferenceToDerived(Type& value) {  // NOLINT
    DerivedType* ptr = static_cast<DerivedType*>(&value);
    CHECK_NOTNULL(ptr);
    return *ptr;
  }
  static const DerivedType& getReferenceToDerived(
      const Type& value) {  // NOLINT
    const DerivedType* ptr = static_cast<const DerivedType*>(&value);
    CHECK_NOTNULL(ptr);
    return *ptr;
  }
  static void transferOwnership(std::shared_ptr<ElementType> object,
                                DerivedType* destination) {
    *destination = *object;
  }
};
template <typename Type, typename DerivedType>
struct InstanceFactory<true, Type, DerivedType> {
  typedef typename DerivedType::element_type ElementType;
  static typename Type::element_type* getPointerTo(Type& value) {  // NOLINT
    CHECK(value != nullptr);
    return value.get();
  }
  static typename Type::element_type& getReferenceTo(Type& value) {  // NOLINT
    CHECK(value != nullptr);
    return *value;
  }
  static const typename Type::element_type& getReferenceTo(
      const Type& value) {  // NOLINT
    CHECK(value != nullptr);
    return *value;
  }
  static typename DerivedType::element_type* getPointerToDerived(
      Type& value) {  // NOLINT
    CHECK(value != nullptr);
    typename DerivedType::element_type* ptr =
        static_cast<typename DerivedType::element_type*>(value.get());
    CHECK_NOTNULL(ptr);
    return ptr;
  }
  static typename DerivedType::element_type& getReferenceToDerived(
      Type& value) {  // NOLINT
    CHECK(value != nullptr);
    typename DerivedType::element_type* ptr =
        static_cast<typename DerivedType::element_type*>(value.get());
    CHECK_NOTNULL(ptr);
    return *ptr;
  }
  static const typename DerivedType::element_type& getReferenceToDerived(
      const Type& value) {  // NOLINT
    CHECK(value != nullptr);
    const typename DerivedType::element_type* ptr =
        static_cast<const typename DerivedType::element_type*>(value.get());
    CHECK_NOTNULL(ptr);
    return *ptr;
  }
  static void transferOwnership(std::shared_ptr<ElementType> object,
                                Type* destination) {
    *destination = object;
  }
};
}  // namespace traits

class ChunkManagerBase;
class NetTable;

/**
 * Needs to be implemented by applications.
 */
template <typename ObjectType>
std::shared_ptr<ObjectType> objectFromRevision(
    const map_api::Revision& revision);
template <typename ObjectType>
void objectToRevision(const ObjectType& object, map_api::Revision* revision);

template <typename IdType, typename ObjectType>
void objectToRevision(const IdType id, const ObjectType& object,
                      map_api::Revision* revision) {
  CHECK_NOTNULL(revision);
  objectToRevision(object, revision);
  IdType present_id = revision->getId<IdType>();
  if (present_id.isValid()) {
    CHECK_EQ(id, present_id);
  } else {
    revision->setId(id);
  }
}

/**
 * IdType needs to be a UniqueId.
 * The type Value is the type of the actual container objects.
 * The type DerivedValue is the type of the objects to be newly constructed.
 */
template <typename IdType, typename Value, typename DerivedValue = Value>
class Cache : public CacheBase,
              public common::MappedContainerBase<IdType, Value> {
 public:
  typedef std::shared_ptr<Cache<IdType, Value, DerivedValue> > Ptr;
  typedef std::shared_ptr<const Cache<IdType, Value, DerivedValue> > ConstPtr;

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
   * Will cache revision of object.
   */
  bool has(const IdType& id) const;

  /**
   * Available with the currently active set of chunks.
   */
  void getAllAvailableIds(std::vector<IdType>* available_ids) const;

  size_t size() const;
  bool empty() const;

 private:
  static constexpr bool kIsPointer = common::IsPointerType<Value>::value;
  typedef traits::InstanceFactory<kIsPointer, Value, DerivedValue> Factory;

  /**
   * Mutex MUST be locked prior to calling the getRevisionLocked functions.
   */
  std::shared_ptr<const Revision> getRevisionLocked(const IdType& id) const;
  virtual void prepareForCommit() override;

  struct ValueHolder {
    ValueHolder(const Value& _value, bool _dirty) :
      value(_value), dirty(_dirty) { }
    Value value;
    bool dirty;
  };

  typedef std::unordered_map<IdType, ValueHolder> CacheMap;
  typedef std::unordered_set<IdType> IdSet;
  typedef std::vector<IdType> IdVector;

  mutable CacheMap cache_;
  mutable CRTable::RevisionMap revisions_;
  IdSet removals_;
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

  class AvailableIds {
   public:
    AvailableIds(NetTable* underlying_table,
                 TransactionAccessFactory* transaction);
    const IdVector& getAllIds() const;
    bool hasId(const IdType& id) const;
    void addId(const IdType& id);
    void removeId(const IdType& id);

   private:
    void getAvailableIdsLocked() const;
    mutable IdVector ordered_available_ids_;
    mutable IdSet available_ids_;
    mutable bool ids_fetched_;
    NetTable* underlying_table_;
    TransactionAccessFactory* transaction_;
  };
  AvailableIds available_ids_;

  mutable std::mutex mutex_;
  typedef std::lock_guard<std::mutex> LockGuard;
};

}  // namespace map_api

#include "./map-api/cache-inl.h"

#endif  // MAP_API_CACHE_H_
