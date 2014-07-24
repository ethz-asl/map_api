#ifndef MAP_API_CHUNK_TRANSACTION_H_
#define MAP_API_CHUNK_TRANSACTION_H_

#include <memory>
#include <unordered_map>

#include "map-api/cr-table.h"
#include "map-api/id.h"
#include "map-api/revision.h"
#include "map-api/logical-time.h"

namespace map_api {

/**
 * This class is somewhat weaker than the first transaction draft
 * (LocalTransaction, now deprecated) because conflict checking and
 * committing is handled in the Chunk class.
 */
class ChunkTransaction {
 public:
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  template <typename ValueType>
  void addConflictCondition(const std::string& key, const ValueType& value);
  // TODO(tcies) conflict conditions
  std::shared_ptr<Revision> getById(const Id& id);
  std::shared_ptr<Revision> getByIdFromUncommitted(const Id& id) const;
  template <typename ValueType>
  std::shared_ptr<Revision> findUnique(
      const std::string& key, const ValueType& value);
  // TODO(tcies) all other flavors of reading

 private:
  ChunkTransaction(const LogicalTime& begin_time, CRTable* cache);
  ChunkTransaction(const ChunkTransaction&) = delete;
  ChunkTransaction& operator =(const ChunkTransaction&) = delete;
  friend class Chunk;

  /**
   * Strong typing of table operation maps.
   */
  class InsertMap : public std::unordered_map<Id, std::shared_ptr<Revision> > {
  };
  class UpdateMap : public std::unordered_map<Id, std::shared_ptr<Revision> > {
  };
  struct ConflictCondition {
    const std::string key;
    const std::shared_ptr<Revision> value_holder;
    ConflictCondition(
        const std::string& _key,
        const std::shared_ptr<Revision>& _value_holder) : key(_key),
            value_holder(_value_holder) {}
  };
  class ConflictVector : public std::vector<ConflictCondition> {};
  InsertMap insertions_;
  UpdateMap updates_;
  ConflictVector conflict_conditions_;
  LogicalTime begin_time_;
  CRTable* cache_;
  std::shared_ptr<const Revision> structure_reference_;
};

} /* namespace map_api */

#include "map-api/chunk-transaction-inl.h"

#endif /* MAP_API_CHUNK_TRANSACTION_H_ */
