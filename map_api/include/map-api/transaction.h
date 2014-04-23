/*
 * transaction.h
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <map>
#include <set>
#include <queue>
#include <memory>
#include <mutex>

#include "map-api/cr-table-interface.h"
#include "map-api/cru-table-interface.h"
#include "map-api/hash.h"
#include "map-api/revision.h"
#include "map-api/time.h"

namespace map_api {

class Transaction {
 public:
  typedef std::shared_ptr<Revision> SharedRevisionPointer;

  Transaction(const Hash& owner);

  bool begin();
  bool commit();
  bool abort();

  /**
   * Sets a hash ID for the table to be inserted. Returns that ID, such that
   * the item can be subsequently referred to.
   *
   * Item can't const because of un-constability due to auto-indexing of
   * revisions.
   */
  template<typename TableInterfaceType>
  Hash insert(TableInterfaceType& table,
              const SharedRevisionPointer& item);
  /**
   * Fails if global state differs from groundState before updating
   */
  bool update(CRUTableInterface& table, const Hash& id,
              const SharedRevisionPointer& newRevision);

  /**
   * Returns latest revision prior to transaction begin time
   */
  template<typename TableInterfaceType>
  SharedRevisionPointer read(TableInterfaceType& table, const Hash& id);
  /**
   * Define own fields for database tables, such as for locks.
   */
  // static std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // requiredTableFields();
  // TODO(tcies) later, start with mutexes
 private:
  class CRItemIdentifier : public std::pair<const CRTableInterface&, Hash>{
   public:
    inline CRItemIdentifier(const CRTableInterface& table,
                            const Hash& id) :
                            std::pair<const CRTableInterface&, Hash>(table,id)
                            {}
    // required for set
    inline bool operator <(const CRItemIdentifier& other) const{
      if (first.name() == other.first.name())
        return second < other.second;
      return first.name() < other.first.name();
    }

  };
  class CRUItemIdentifier : public std::pair<const CRUTableInterface&, Hash>{
   public:
    inline CRUItemIdentifier(const CRUTableInterface& table,
                             const Hash& id) :
                             std::pair<const CRUTableInterface&, Hash>(table,id)
                             {}
    // required for map
    inline bool operator <(const CRUItemIdentifier& other) const{
      if (first.name() == other.first.name())
        return second < other.second;
      return first.name() < other.first.name();
    }
  };

  typedef std::map<CRItemIdentifier, const SharedRevisionPointer>
  InsertMap;

  typedef std::map<CRUItemIdentifier, const SharedRevisionPointer>
  UpdateMap;

  bool notifyAbortedOrInactive();
  /**
   * Returns true if the supplied map (insert or update) has a conflict
   */
  template<typename Map>
  bool hasMapConflict(const Map& map);
  /**
   * Returns true if the supplied insert/update request has a conflict
   */
  template<typename Identifier>
  bool hasItemConflict(const Identifier& item);

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

  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
  bool active_;
  bool aborted_;
  Time beginTime_;

  /**
   * Mutex for db access... for now
   */
  static std::recursive_mutex dbMutex_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
