#ifndef MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_
#define MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_

#include <vector>

#include <multiagent-mapping-common/mapped-container-base.h>

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
    *CHECK_NOTNULL(available_ids) = available_ids_;
  }

  virtual size_t size() const final override { return available_ids_.size(); }

  virtual bool empty() const final override { return available_ids_.empty(); }

  virtual std::shared_ptr<const Revision>& getMutable(const IdType& id)
      final override {
    return transaction_->getUpdateEntry(id, table_);
  }

  virtual const std::shared_ptr<const Revision> get(const IdType& id) const
      final override {
    return transaction_->getById(id, table_);
  }

  virtual bool insert(const IdType& id,
                      const std::shared_ptr<const Revision>& value)
      final override {
    CHECK_EQ(value->getId<IdType>(), id);
    // Const casts are always unfortunate. To fix the following, however,
    // changes would need to be made deep inside Map API. In particular, it
    // would probably be best to split Revision into RevisionPayload and
    // RevisionMetadata, such that transaction functions can take const
    // payloads. At the moment these functions need to take non-const revisions
    // in order to
    return transaction_->insert(chunk_manager_,
                                std::const_pointer_cast<Revision>(value));
  }

  virtual void erase(const IdType& id) final override {
    transaction_->remove(table_, id);
  }

  void refresh() { transaction_->getAvailableIds(table_, &available_ids_); }

 private:
  Transaction* const transaction_;
  NetTable* const table_;
  ChunkManagerBase* const chunk_manager_;

  std::vector<IdType> available_ids_;
};

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_TRANSACTION_INTERFACE_H_
