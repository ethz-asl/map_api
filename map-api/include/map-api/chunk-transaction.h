#ifndef MAP_API_CHUNK_TRANSACTION_H_
#define MAP_API_CHUNK_TRANSACTION_H_

#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "map-api/cr-table.h"
#include "map-api/logical-time.h"
#include "map-api/revision.h"

namespace map_api {
class Chunk;
class Id;

/**
 * This class is somewhat weaker than the first transaction draft
 * (LocalTransaction, now deprecated) because conflict checking and
 * committing is handled in the Chunk class.
 */
class ChunkTransaction {
  friend class NetTableTransaction;
  friend class Transaction;      // for internal typedefs
  friend class NetTableManager;  // metatable works directly with this
  friend class NetTableFixture;
  FRIEND_TEST(NetTableFixture, ChunkTransactions);
  FRIEND_TEST(NetTableFixture, ChunkTransactionsConflictConditions);

 private:
  explicit ChunkTransaction(Chunk* chunk);
  ChunkTransaction(const LogicalTime& begin_time, Chunk* chunk);

  // READ
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id);
  template <typename IdType>
  std::shared_ptr<const Revision> getByIdFromUncommitted(const IdType& id)
      const;
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(int key, const ValueType& value);
  CRTable::RevisionMap dumpChunk();

  // WRITE
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);
  template <typename ValueType>
  void addConflictCondition(int key, const ValueType& value);

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
  class InsertMap : public CRTable::NonConstRevisionMap {};
  class UpdateMap : public CRTable::NonConstRevisionMap {};
  class RemoveMap : public CRTable::NonConstRevisionMap {};
  struct ConflictCondition {
    const int key;
    const std::shared_ptr<Revision> value_holder;
    ConflictCondition(int _key, const std::shared_ptr<Revision>& _value_holder)
        : key(_key), value_holder(_value_holder) {}
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

#include "./chunk-transaction-inl.h"

#endif  // MAP_API_CHUNK_TRANSACTION_H_
