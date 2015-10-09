#ifndef MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_
#define MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_

#include <vector>

#include <multiagent-mapping-common/mapped-container-base.h>

#include "map-api/transaction.h"

namespace map_api {

template <typename IdType>
class NetTableTransactionInterface
    : public common::MappedContainerBase<IdType,
                                         std::shared_ptr<const Revision>> {
 public:
  NetTableTransactionInterface(Transaction* const transaction,
                               NetTable* const table,
                               ChunkManagerBase* const chunk_manager)
      : transaction_(CHECK_NOTNULL(transaction)),
        table_(CHECK_NOTNULL(table)),
        chunk_manager_(CHECK_NOTNULL(chunk_manager)) {
    refresh();
  }

  virtual bool has(const IdType& id) const final override {
    return available_ids_.count(id) != 0;
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

  virtual const std::shared_ptr<const Revision> get(const IdType& id) const
      final override {
    std::shared_ptr<const Revision> result = transaction_->getById(id, table_);
    CHECK(result);
    return result;
  }

  virtual bool insert(const IdType& id,
                      const std::shared_ptr<const Revision>& value)
      final override {
    std::const_pointer_cast<Revision>(value)->setId<IdType>(id);
    // Const casts are always unfortunate. To fix the following, however,
    // changes would need to be made deep inside Map API. In particular, it
    // would probably be best to split Revision into RevisionPayload and
    // RevisionMetadata, such that transaction functions can take const
    // payloads. At the moment these functions need to take non-const revisions
    // in order to
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

#endif  // MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_
