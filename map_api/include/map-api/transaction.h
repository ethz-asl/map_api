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
  typedef std::pair<CRUItemIdentifier, const SharedRevisionPointer>
  UpdateRequest;
  typedef std::deque<UpdateRequest> UpdateQueue;
  typedef std::pair<CRTableInterface&, const SharedRevisionPointer>
  CRInsertRequest;
  typedef std::deque<CRInsertRequest> CRInsertQueue;
  typedef std::pair<CRUTableInterface&, const SharedRevisionPointer>
  CRUInsertRequest;
  typedef std::deque<CRUInsertRequest> CRUInsertQueue;

  /**
   * Type for keeping track of previous changes within the same transaction
   * Using pointers to allow NULL pointer as "invalid"
   */
  typedef std::map<CRItemIdentifier, CRInsertRequest*> CRUpdateState;
  typedef std::map<CRUItemIdentifier, UpdateRequest*> CRUUpdateState;


  bool notifyAbortedOrInactive();
  /**
   * Returns true if the supplied queue has a conflict, keeps track of the
   * update state of the transaction: Operations are registered, such that
   * subsequent operations on the same item or insert conflicts are recognized
   * properly.
   */
  template<typename Queue, typename UpdateState>
  bool hasQueueConflict(const Queue& queue, UpdateState& state);
  /**
   * Returns true if the supplied insert/update request has a conflict
   */
  template<typename Request, typename UpdateState>
  bool hasRequestConflict(Request& request, UpdateState& state);
  /**
   * Templateable common operations for insert conflict checking
   */
  template<typename Request, typename UpdateState, typename Identifier>
  bool hasInsertRequestConflictCommons(const Request& request,
                                    UpdateState& state, Hash& id);

  /**
   * Update queue: Queue of update queries requested over the course of the
   * transaction, to be commited at the end. These must be applied
   * in consistent order as the same item might be updated twice, thus queue
   */
  UpdateQueue updateQueue_;

  /**
   * Insert queues: Queues of insert queries requested over the course of the
   * transaction, to be commited at the end. Order doesn't matter here,
   * however, all inserts must be committed before updates.
   */
  CRInsertQueue crInsertQueue_;
  /**
   * CRU inserts are split into two parts: Insertion of item pointing to no
   * revision, then update to revision.
   */
  CRUInsertQueue cruInsertQueue_;
  /**
   * Update states to keep track of uncommitted changes within the transaction
   */
  CRUpdateState crUpdateState_;
  CRUUpdateState cruUpdateState_;

  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
  bool active_;
  bool aborted_;
  Time beginTime_;

  /**
   * Mutex for db access... for now
   */
  static std::mutex dbMutex_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
