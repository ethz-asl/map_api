/*
 * transaction.h
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <queue>
#include <memory>

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
              SharedRevisionPointer& item);
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
  bool notifyAbortedOrInactive();

  /**
   * Update queue:  Keeps uncommitted history entries. These must be applied
   * in consistent order as the same item might be updated twice, thus queue
   */
  typedef std::pair<CRUTableInterface&, Hash> ItemIdentifier;
  typedef std::pair<ItemIdentifier, SharedRevisionPointer> UpdateTodo;
  std::queue<UpdateTodo> updateQueue_;

  /**
   * Insert queues: Uncommitted initial revisions. Order doesn't matter here,
   * however, all inserts must be committed before updates.
   */
  typedef std::pair<CRTableInterface&, SharedRevisionPointer>
  CRInsertTodo;
  std::queue<CRInsertTodo> crInsertQueue_;
  /**
   * CRU inserts are split into two parts: Insertion of item pointing to no
   * revision, then update to revision.
   */
  typedef std::pair<CRUTableInterface&, SharedRevisionPointer>
  CRUInsertTodo;
  std::queue<CRUInsertTodo> cruInsertQueue_;
  /**
   * TODO(tcies) will also need a map for keeping track of the latest
   * revision Hash of each modified object, in case it gets updated twice.
   */

  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
  bool active_;
  bool aborted_;
  Time beginTime_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
