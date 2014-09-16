#ifndef MAP_API_LOCAL_TRANSACTION_H_
#define MAP_API_LOCAL_TRANSACTION_H_

#include <map>
#include <set>
#include <queue>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "map-api/cr-table.h"
#include "map-api/cru-table.h"
#include "map-api/item-id.h"
#include "map-api/revision.h"
#include "map-api/logical-time.h"

namespace map_api {

/**
 * This transaction class has only been here for Transaction proof-of concept
 * work on a single process; TODO(tcies) it should eventually be removed when
 * moving to proper networking.
 */
class LocalTransaction {
 public:
  typedef std::shared_ptr<Revision> SharedRevisionPointer;

  bool begin();
  bool commit();
  bool abort();

  /**
   * Sets a hash ID for the table to be inserted. Returns that ID, such that
   * the item can be subsequently referred to.
   */
  Id insert(const SharedRevisionPointer& item, CRTable* table);

  /**
   * Allows the user to preset a Hash ID. Will fail in commit if there is a
   * conflict.
   */
  bool insert(const Id& id, const SharedRevisionPointer& item,
              CRTable* table);

  /**
   * Transaction will fail if a table item where key = value exists. The
   * necessity of this function has arisen from MapApiCore::syncTableDefinition,
   * where we only want to create a table definition if it is not yet present.
   * We need to use the base class CRTableInterface because partial template
   * specialization of functions is not allowed in C++.
   * In commit(), conflicts will be looked for in the table only and not in the
   * uncommited data, because this function may be used in conjunction with
   * insertion of data that would cause a conflict (e.g.
   * MapApiCore::syncTableDefinition)
   */
  template <typename ValueType>
  bool addConflictCondition(int key, const ValueType& value, CRTable* table);

  /**
   * Returns latest revision prior to transaction begin time
   */
  SharedRevisionPointer read(const Id& id, CRTable* table);

  /**
   * Returns latest revision prior to transaction begin time for all contents
   */
  bool dumpTable(CRTable* table, CRTable::RevisionMap* dest);

  /**
   * Fails if global state differs from groundState before updating
   */
  bool update(const Id& id, const SharedRevisionPointer& newRevision,
              CRUTable* table);

  /**
   * Looks for items where key = value. As with addConflictCondition(),
   * CRTableInterface because partial template
   * specialization of functions is not allowed in C++.
   */
  template <typename ValueType>
  int find(int key, const ValueType& value, CRTable* table,
           CRTable::RevisionMap* dest) const;
  /**
   * Same as find(), but ensuring that there is only one result
   */
  template<typename ValueType>
  SharedRevisionPointer findUnique(
      const std::string& key, const ValueType& value, CRTable* table) const;
  /**
   * Define own fields for database tables, such as for locks.
   */
  // static std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // requiredTableFields();
  // TODO(tcies) later, start with mutexes
 private:
  /**
   * Same as functionality as find(), but looks only in the uncommitted
   * (insertions and updates) revisions. Consequently, this is used in find(),
   * among others. If key is an empty string, no filter will be applied.
   */
  template <typename ValueType>
  int findInUncommitted(
      const CRTable& table, int key, const ValueType& value,
      std::unordered_map<Id, SharedRevisionPointer>* dest) const;
  template<typename ValueType>
  SharedRevisionPointer findUniqueInUncommitted(
      const CRTable& table, const std::string& key, const ValueType& value)
  const;

  class InsertMap : public std::map<ItemId, const SharedRevisionPointer> {};
  class UpdateMap : public std::map<ItemId, const SharedRevisionPointer> {};

  bool notifyAbortedOrInactive() const;
  /**
   * Returns true if the supplied insert/update request has a conflict
   */
  template <typename Identifier>
  bool hasItemConflict(Identifier* item);
  /**
   * Returns true if the supplied container has a conflict
   */
  template <typename Container>
  inline bool hasContainerConflict(Container* container);

  /**
   * Maps of insert queries requested over the course of the
   * transaction, to be committed at the end.
   * All inserts must be committed before updates.
   */
  InsertMap insertions_;

  /**
   * Map of update queries requested over the course of the
   * transaction, to be committed at the end. If an item gets updated multiple
   * times, only the latest revision will be committed
   */
  UpdateMap updates_;

  /**
   * A conflict condition leads to a conflict if the key-value pair it describes
   * is found in the table in specifies.
   * The value is stored in a Revision in order to easily allow it to take any
   * type, thanks to the Revision template specializations.
   */
  struct ConflictCondition {
    const int key;
    const SharedRevisionPointer valueHolder;
    CRTable* table;
    ConflictCondition(int _key, const SharedRevisionPointer& _valueHolder,
                      CRTable* _table)
        : key(_key), valueHolder(_valueHolder), table(CHECK_NOTNULL(_table)) {}
  };
  typedef std::vector<ConflictCondition> ConflictConditionVector;
  ConflictConditionVector conflictConditions_;

  bool active_ = false;
  bool aborted_ = false;
  LogicalTime beginTime_;

  /**
   * Mutex for db access... for now
   */
  static std::recursive_mutex dbMutex_;
};

}  // namespace map_api

#include "map-api/local-transaction-inl.h"

#endif  // MAP_API_LOCAL_TRANSACTION_H_
