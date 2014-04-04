/*
 * transaction.h
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include <stack>
#include <memory>

#include "map-api/hash.h"
#include "map-api/revision.h"

namespace map_api {

class Transaction {
 public:
  typedef std::shared_ptr<Revision> SharedRevisionPointer;
  /**
   * Exception-free initialization.
   */
  Transaction(const Hash& owner);
  /**
   * Any other initialization
   */
  bool begin();
  bool commit();
  bool abort();
  /**
   * Passing shared pointer so we can be more flexible with the journal.
   */
  bool addInsertQuery(const SharedRevisionPointer& query);
  /**
   * Transaction fails if global state differs from groundState before updating
   */
  bool addUpdateQuery(const SharedRevisionPointer& oldState,
                      const SharedRevisionPointer& newState);
  /**
   * Does a select query need to be in a transaction? What would rollback mean?
   * Cache invalidation of some sort?
   */
  SharedRevisionPointer addSelectQuery(
      const std::string& table, const Hash& id);
  /**
   * Define own fields for database tables, such as for locks.
   */
  static std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  requiredTableFields();
 private:
  /**
   * Common operations for insert/update query
   */
  bool commonOperations(const SharedRevisionPointer& oldState,
      const SharedRevisionPointer& newState);
  bool notifyAbortedOrInactive();
  /**
   * Journal entry
   */
  typedef struct JournalEntry{
    SharedRevisionPointer oldState;
    SharedRevisionPointer newState;
    JournalEntry(){}
    JournalEntry(const SharedRevisionPointer& old_state,
                 const SharedRevisionPointer& new_state) : oldState(old_state),
                     newState(new_state) {}
  }JournalEntry;
  /**
   * Journal: stack, as the latest changes need to be rolled back first.
   */
  std::stack<JournalEntry> journal_;
  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
  bool active_;
  bool aborted_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
