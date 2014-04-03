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
#include "map-api/table-insert-query.h"

namespace map_api {

class Transaction {
 public:
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
  bool addInsertQuery(std::shared_ptr<const TableInsertQuery> query);
  /**
   * Transaction fails if global state differs from groundState before updating
   */
  bool addUpdateQuery(
      std::shared_ptr<const TableInsertQuery> groundState,
      std::shared_ptr<const TableInsertQuery> update);
  /**
   * Does a select query need to be in a transaction? What would rollback mean?
   * Cache invalidation of some sort?
   */
  std::shared_ptr<TableInsertQuery> addSelectQuery(
      const std::string& table, const Hash& id);
  /**
   * Define own fields for database tables, such as for locks.
   */
  static std::shared_ptr<std::vector<std::string> >
  requiredTableFields();
 private:
  /**
   * Journal entry
   */
  typedef struct{
    std::shared_ptr<const TableInsertQuery> oldState;
    std::shared_ptr<const TableInsertQuery> newState;
  }JournalEntry;
  /**
   * Journal: stack, as the latest changes need to be rolled back first.
   */
  std::stack<JournalEntry> journal_;
  Hash owner_;
  std::shared_ptr<Poco::Data::Session> session_;
};

} /* namespace map_api */

#endif /* TRANSACTION_H_ */
