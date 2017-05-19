#ifndef DMAP_COMMON_MAPPED_CONTAINER_BASE_H_
#define DMAP_COMMON_MAPPED_CONTAINER_BASE_H_

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "dmap-common/traits.h"

namespace dmap_common {
template <typename IdType, typename Value>
class MappedContainerBase {
 public:
  // Useful for the special case where Value is a shared_ptr; this will make
  // get() return a shared_ptr to const instead of a const ref to a shared ptr.
  typedef typename dmap_common::IsPointerType<Value>::const_ref_type
      ConstRefReturnType;

  typedef std::pair<IdType, Value> value_type;
  virtual ~MappedContainerBase() {}
  void shallowCopyFrom(MappedContainerBase<IdType, Value>* other) {
    CHECK_NOTNULL(other);
    std::vector<IdType> ids;
    other->getAllAvailableIds(&ids);
    for (const IdType& id : ids) {
      insert(id, other->getMutable(id));
    }
  }

  virtual bool has(const IdType& id) const = 0;
  virtual void getAllAvailableIds(
      std::vector<IdType>* available_ids) const = 0;
  void forEach(const std::function<void(ConstRefReturnType value)>& action) const {
    std::vector<IdType> available_ids;
    getAllAvailableIds(&available_ids);
    for (const IdType& id : available_ids) {
      action(get(id));
    }
  }
  void forEachWhile(const std::function<
      bool(const IdType& id, const Value& value)>& action) const {  // NOLINT
    std::vector<IdType> available_ids;
    getAllAvailableIds(&available_ids);
    for (const IdType& id : available_ids) {
      if (!action(id, get(id))) {
        break;
      }
    }
  }
  virtual size_t size() const = 0;
  virtual bool empty() const = 0;

  virtual Value& getMutable(const IdType& id) = 0;
  virtual ConstRefReturnType get(const IdType& id) const = 0;
  Value& getAnyMutable() {
    std::vector<IdType> available_ids;
    getAllAvailableIds(&available_ids);
    CHECK(!available_ids.empty());
    return getMutable(available_ids.front());
  }

  virtual bool insert(const IdType& id, const Value& value) = 0;
  virtual void erase(const IdType& id) = 0;
};

template <typename IdType, typename Value>
class HashMapContainer : public MappedContainerBase<IdType, Value> {
 public:
  typedef std::shared_ptr<HashMapContainer<IdType, Value> > Ptr;
  typedef std::shared_ptr<const HashMapContainer<IdType, Value> > ConstPtr;
  virtual ~HashMapContainer() {}
  virtual Value& getMutable(const IdType& id) final override {
    typename MapType::iterator it = map_.find(id);
    CHECK(it != map_.end());
    return it->second;
  }
  virtual typename MappedContainerBase<IdType, Value>::ConstRefReturnType get(
      const IdType& id) const final override {
    typename MapType::const_iterator it = map_.find(id);
    CHECK(it != map_.end());
    return it->second;
  }
  virtual bool insert(const IdType& id, const Value& value) final override {
    CHECK(map_.emplace(id, value).second);
    return true;
  }
  virtual void erase(const IdType& id) final override { map_.erase(id); }
  virtual bool has(const IdType& id) const final override {
    typename MapType::const_iterator found = this->map_.find(id);
    return found != map_.end();
  }
  virtual void getAllAvailableIds(std::vector<IdType>* available_ids) const
      final override {
    CHECK_NOTNULL(available_ids);
    available_ids->clear();
    available_ids->reserve(map_.size());
    for (const typename MapType::value_type& pair : map_) {
      available_ids->emplace_back(pair.first);
    }
  }
  virtual size_t size() const final override { return map_.size(); }
  virtual bool empty() const final override { return map_.empty(); }

 private:
  typedef std::unordered_map<IdType, Value> MapType;
  MapType map_;
};
}  // namespace dmap_common
#endif  // DMAP_COMMON_MAPPED_CONTAINER_BASE_H_
