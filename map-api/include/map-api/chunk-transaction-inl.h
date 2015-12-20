#ifndef MAP_API_CHUNK_TRANSACTION_INL_H_
#define MAP_API_CHUNK_TRANSACTION_INL_H_

#include <string>
#include <utility>
#include <vector>

#include <multiagent-mapping-common/unique-id.h>

namespace map_api {

template <typename ValueType>
void ChunkTransaction::addConflictCondition(int key, const ValueType& value) {
  std::shared_ptr<Revision> value_holder =
      chunk_->data_container_->getTemplate();
  value_holder->set(key, value);
  conflict_conditions_.push_back(ConflictCondition(key, value_holder));
}

template <typename IdType>
std::shared_ptr<const Revision> ChunkTransaction::getById(const IdType& id) {
  return combined_view_.get(id.template toIdType<common::Id>());
}

template <typename IdType>
void ChunkTransaction::getAvailableIds(std::unordered_set<IdType>* ids) {
  CHECK_NOTNULL(ids)->clear();
  std::unordered_set<common::Id> common_ids;
  combined_view_.getAvailableIds(&common_ids);
  for (const common::Id& id : common_ids) {
    ids->emplace(id.template toIdType<IdType>());
  }
}

template <typename IdType>
void ChunkTransaction::getMutableUpdateEntry(
    const IdType& id, std::shared_ptr<const Revision>** result) {
  CHECK_NOTNULL(result);
  common::Id common_id = id.template toIdType<common::Id>();
  if (!delta_.getMutableUpdateEntry(common_id, result)) {
    std::shared_ptr<const Revision> original = getById(id);
    CHECK(original);
    std::shared_ptr<Revision> to_emplace;
    original->copyForWrite(&to_emplace);
    update(to_emplace);
    CHECK(delta_.getMutableUpdateEntry(common_id, result));
  }
}

template <typename ValueType>
std::shared_ptr<const Revision> ChunkTransaction::findUnique(
    int key, const ValueType& value) {
  // FIXME(tcies) Also search in uncommitted.
  // FIXME(tcies) Also search in previously committed.
  std::shared_ptr<const Revision> result =
      chunk_->constData()->findUnique(key, value, begin_time_);
  return result;
}

}  // namespace map_api

#endif  // MAP_API_CHUNK_TRANSACTION_INL_H_
