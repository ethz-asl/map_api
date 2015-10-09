#ifndef MAP_API_OBJECT_CACHE_H_
#define MAP_API_OBJECT_CACHE_H_

#include <vector>

namespace map_api {

// This is a threadsafe MappedContainerBase implementation intended for use by
// Map API applications. It can be obtained using Transaction::createCache().
template <typename IdType, typename ObjectType>
class ObjectCache : public common::MappedContainerBase<IdType, ObjectType> {
 public:
  ObjectCache(Transaction* const transaction, NetTable* const table,
              ChunkManagerBase* const chunk_manager)

  virtual bool has(const IdType& id) const = 0;
  virtual void getAllAvailableIds(std::vector<IdType>* available_ids) const = 0;
  virtual size_t size() const = 0;
  virtual bool empty() const = 0;

  virtual Value& getMutable(const IdType& id) = 0;
  virtual ConstRefReturnType get(const IdType& id) const = 0;

  virtual bool insert(const IdType& id, const Value& value) = 0;
  virtual void erase(const IdType& id) = 0;

 private:
  NetTable* const table_;
  NetTableTransactionInterface<IdType> transaction_interface_;
  ObjectAndMetadataCache<IdType, ObjectType> cache_;
};

}  // namespace map_api

#endif  // MAP_API_OBJECT_CACHE_H_
