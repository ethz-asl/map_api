#ifndef MAP_API_CHUNK_TRANSACTION_H_
#define MAP_API_CHUNK_TRANSACTION_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "map-api/chunk.h"
#include "map-api/cr-table.h"
#include "map-api/logical-time.h"
#include "map-api/revision.h"
#include "map-api/unique-id.h"

namespace map_api {

/**
 * This class is somewhat weaker than the first transaction draft
 * (LocalTransaction, now deprecated) because conflict checking and
 * committing is handled in the Chunk class.
 */
class ChunkTransaction {
  friend class NetTableTransaction;
  friend class Transaction;      // for internal typedefs
  friend class NetTableManager;  // metatable works directly with this
  friend class NetTableTest;
  FRIEND_TEST(NetTableTest, ChunkTransactions);
  FRIEND_TEST(NetTableTest, ChunkTransactionsConflictConditions);

 private:
  explicit ChunkTransaction(Chunk* chunk);
  ChunkTransaction(const LogicalTime& begin_time, Chunk* chunk);

  // READ
  template <typename IdType>
  std::shared_ptr<Revision> getById(const IdType& id);
  template <typename IdType>
  std::shared_ptr<Revision> getByIdFromUncommitted(const IdType& id) const;
  template <typename ValueType>
  std::shared_ptr<Revision> findUnique(
      const std::string& key, const ValueType& value);
  CRTable::RevisionMap dumpChunk();

  // WRITE
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);
  template <typename ValueType>
  void addConflictCondition(const std::string& key, const ValueType& value);

  // TRANSACTION OPERATIONS
  bool commit();
  bool check();
  void checkedCommit(const LogicalTime& time);
  struct Conflict {
    const std::shared_ptr<const Revision> theirs;
    const std::shared_ptr<const Revision> ours;
  };
  // constant splicing, linear iteration
  typedef std::list<Conflict> Conflicts;
  /**
   * Merging and changeCount are not compatible with conflict conditions.
   */
  void merge(const std::shared_ptr<ChunkTransaction>& merge_transaction,
             Conflicts* conflicts);
  size_t numChangedItems() const;

  // INTERNAL
  void prepareCheck(const LogicalTime& check_time,
                    std::unordered_map<Id, LogicalTime>* chunk_stamp);

  /**
   * Strong typing of table operation maps.
   */
  class InsertMap : public CRTable::RevisionMap {};
  class UpdateMap : public CRTable::RevisionMap {};
  class RemoveMap : public CRTable::RevisionMap {};
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
  RemoveMap removes_;
  ConflictVector conflict_conditions_;
  LogicalTime begin_time_;
  Chunk* chunk_;
  std::shared_ptr<const Revision> structure_reference_;
};

}  // namespace map_api

#include "map-api/chunk-transaction-inl.h"

#endif  // MAP_API_CHUNK_TRANSACTION_H_
