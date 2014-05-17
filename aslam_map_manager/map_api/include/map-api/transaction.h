#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <map>
#include <set>
#include <queue>
#include <memory>
#include <mutex>

#include "map-api/cr-table-interface.h"
#include "map-api/cru-table-interface.h"
#include "map-api/revision.h"
#include "map-api/time.h"

namespace map_api {

class Transaction {
 public:
  typedef std::shared_ptr<Revision> SharedRevisionPointer;

  bool begin();
  bool commit();
  bool abort();

  /**
   * Sets a hash ID for the table to be inserted. Returns that ID, such that
   * the item can be subsequently referred to.
   */
  template<typename TableInterfaceType>
  Id insert(TableInterfaceType& table,
            const SharedRevisionPointer& item);

  /**
   * Allows the user to preset a Hash ID. Will fail in commit if there is a
   * conflict.
   */
  template<typename TableInterfaceType>
  bool insert(TableInterfaceType& table, const Id& id,
              const SharedRevisionPointer& item);

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
  template<typename ValueType>
  bool addConflictCondition(CRTableInterface& table,
                            const std::string& key, const ValueType& value);

  /**
   * Returns latest revision prior to transaction begin time
   */
  template<typename TableInterfaceType>
  SharedRevisionPointer read(TableInterfaceType& table, const Id& id);

  /**
   * Returns latest revision prior to transaction begin time for all contents
   */
  template<typename TableInterfaceType>
  bool dumpTable(TableInterfaceType& table,
                 std::vector<SharedRevisionPointer>* dest);

  /**
   * Fails if global state differs from groundState before updating
   */
  bool update(CRUTableInterface& table, const Id& id,
              const SharedRevisionPointer& newRevision);

  /**
   * Looks for items where key = value. As with addConflictCondition(),
   * CRTableInterface because partial template
   * specialization of functions is not allowed in C++.
   */
  template<typename ValueType>
  bool find(CRTableInterface& table, const std::string& key,
            const ValueType& value, std::vector<SharedRevisionPointer>* dest)
  const;
  /**
   * Same as find(), but ensuring that there is only one result
   */
  template<typename ValueType>
  SharedRevisionPointer findUnique(CRTableInterface& table,
                                   const std::string& key,
                                   const ValueType& value) const;
  /**
   * Define own fields for database tables, such as for locks.
   */
  // static std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // requiredTableFields();
  // TODO(tcies) later, start with mutexes
 private:
  class CRItemIdentifier : public std::pair<const CRTableInterface&, Id>{
   public:
    inline CRItemIdentifier(const CRTableInterface& table, const Id& id) :
    std::pair<const CRTableInterface&, Id>(table, id) {}
    // required for set
    inline bool operator <(const CRItemIdentifier& other) const{
      if (first.name() == other.first.name())
        return second < other.second;
      return first.name() < other.first.name();
    }

  };
  class CRUItemIdentifier :
      public std::pair<const CRUTableInterface&, Id>{
       public:
    inline CRUItemIdentifier(const CRUTableInterface& table, const Id& id) :
    std::pair<const CRUTableInterface&, Id>(table, id){}
    // required for map
    inline bool operator <(const CRUItemIdentifier& other) const{
      if (first.name() == other.first.name())
        return second < other.second;
      return first.name() < other.first.name();
    }
  };

  typedef std::map<CRItemIdentifier, const SharedRevisionPointer>
  InsertMap;

  typedef std::map<CRUItemIdentifier, SharedRevisionPointer>
  UpdateMap;

  bool notifyAbortedOrInactive() const;
  /**
   * Returns true if the supplied insert/update request has a conflict
   */
  template<typename Identifier>
  bool hasItemConflict(const Identifier& item);
  /**
   * Returns true if the supplied container has a conflict
   */
  template<typename Container>
  inline bool hasContainerConflict(const Container& container);
  template<typename Map>
  inline bool hasMapConflict(const Map& map); // map subspecialization

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
    const CRTableInterface& table;
    const std::string key;
    const SharedRevisionPointer valueHolder;
    ConflictCondition(const CRTableInterface& _table, const std::string& _key,
                      const SharedRevisionPointer& _valueHolder) :
                        table(_table), key(_key), valueHolder(_valueHolder) {}
  };
  typedef std::vector<ConflictCondition> ConflictConditionVector;
  ConflictConditionVector conflictConditions_;

  std::shared_ptr<Poco::Data::Session> session_;
  bool active_ = false;
  bool aborted_ = false;
  Time beginTime_;

  /**
   * Mutex for db access... for now
   */
  static std::recursive_mutex dbMutex_;
};

} /* namespace map_api */

#include "map-api/transaction-inl.h"

#endif /* TRANSACTION_H_ */
