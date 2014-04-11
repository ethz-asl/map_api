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

  template<typename TableInterfaceType>
  bool insert(TableInterfaceType& table,
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
  bool notifyAbortedOrInactive();

  /**
   * Update queue:  Keeps uncommitted history entries. These must be applied
   * in consistent order as the same item might be updated twice, thus queue
   */
  typedef std::pair<CRUTableInterface&, Hash> ItemIdentifier;
  typedef std::pair<ItemIdentifier, Revision> UpdateTodo;
  std::queue<UpdateTodo> updateQueue_;

  /**
   * Insert queues: Uncommitted initial revisions. Order doesn't matter here.
   */
  typedef std::pair<CRTableInterface&, Revision> CRInsertTodo;
  std::queue<CRInsertTodo> crInsertQueue_;
  typedef std::pair<CRUTableInterface&, Revision> CRUInsertTodo;
  std::queue<CRUInsertTodo> cruInsertQueue_;

  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
  bool active_;
  bool aborted_;
  Time startTime_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
