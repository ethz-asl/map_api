#ifndef DMAP_NET_TABLE_TRANSACTION_INTERFACE_H_
#define DMAP_NET_TABLE_TRANSACTION_INTERFACE_H_

#include <vector>

#include <map-api-common/mapped-container-base.h>

#include "map-api/transaction.h"

namespace map_api {

template <typename IdType>
class NetTableTransactionInterface
    : public map_api_common::MappedContainerBase<IdType,
                                         std::shared_ptr<const Revision>> {
 public:
  typedef map_api_common::MappedContainerBase<IdType, std::shared_ptr<const Revision>>
      Base;

  NetTableTransactionInterface(Transaction* const transaction,
                               NetTable* const table,
                               ChunkManagerBase* const chunk_manager)
      : transaction_(CHECK_NOTNULL(transaction)),
        table_(CHECK_NOTNULL(table)),
        chunk_manager_(CHECK_NOTNULL(chunk_manager)) {
    refresh();
  }

  virtual ~NetTableTransactionInterface() {}

  virtual bool has(const IdType& id) const final override {
    return available_ids_.count(id) != 0u;
  }

  virtual void getAllAvailableIds(std::vector<IdType>* available_ids) const
      final override {
    CHECK_NOTNULL(available_ids)->clear();
    refresh();
    available_ids->insert(available_ids->end(), available_ids_.begin(),
                          available_ids_.end());
  }

  virtual size_t size() const final override { return available_ids_.size(); }

  virtual bool empty() const final override { return available_ids_.empty(); }

  virtual std::shared_ptr<const Revision>& getMutable(const IdType& id)
      final override {
    return *transaction_->getMutableUpdateEntry(id, table_);
  }

  virtual typename Base::ConstRefReturnType get(const IdType& id) const
      final override {
    typename Base::ConstRefReturnType result =
        transaction_->getById(id, table_);
    CHECK(result) << "Missing item " << id << " from table " << table_->name();
    CHECK(result->getChunkId().isValid());
    return result;
  }

  virtual bool insert(const IdType& id,
                      const std::shared_ptr<const Revision>& value)
      final override {
    CHECK(id.isValid());
    CHECK(value);
    std::const_pointer_cast<Revision>(value)->setId<IdType>(id);
    // Getting rid of this const cast should be possible, albeit painstaking.
    // Possibilities:
    // * Distinguish ConstRawType and MutableRawType in cache.
    // * Do the data-metadata split at a lower level than ThreadsafeCache.
    transaction_->insert(chunk_manager_,
                         std::const_pointer_cast<Revision>(value));
    return true;
  }

  virtual void erase(const IdType& id) final override {
    transaction_->remove(id, table_);
  }

  void refresh() const {
    std::vector<IdType> available_ids;
    transaction_->getAvailableIds(table_, &available_ids);
    available_ids_.clear();
    available_ids_.insert(available_ids.begin(), available_ids.end());
  }

 private:
  Transaction* const transaction_;
  NetTable* const table_;
  ChunkManagerBase* const chunk_manager_;

  mutable std::unordered_set<IdType> available_ids_;
};

}  // namespace map_api

#endif  // DMAP_NET_TABLE_TRANSACTION_INTERFACE_H_
