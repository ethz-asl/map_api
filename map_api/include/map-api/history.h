/*
 * history.h
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#ifndef HISTORY_H_
#define HISTORY_H_

#include <map-api/table-interface.h>

#include <Poco/DateTime.h>

namespace map_api {

class History : public WriteOnlyTableInterface {
  /**
   * Define the table of which the history is to be kept.
   * Will create a table with the name <table.name()>_history.
   * Owner is used later to verify that owner locks affected rows when pushing
   * a revision to the revision history
   */
  explicit History(const TableInterface& table);
  /**
   * Takes the table name taken from constructor to set up table interface
   */
  virtual bool init();
  /**
   * History table fields:
   * - ID (implicit)
   * - rowID: Hash Identifier of the row that the given revision is a revision
   * of
   * - timestamp: ns-resolution timestamp of the revision
   * - newState: serialized protobuf revision of the updated state
   *
   * TODO(discuss) updateable tables will not contain the actual revision
   * but only a reference to the latest entry in the history table, which then
   * contains the actual data
   */
  virtual bool define();
  /**
   * Returns shared pointer of revision at requested time.
   */
  std::shared_ptr<Revision> revisionAt(const Hash& rowId,
                                       const Poco::DateTime& time);
  /**
   * Directly inserts the new revision into the history at the current time
   * after verifying the lock holder of the row is the same as the owner
   * declared in the constructor. This shall be called exclusively from the
   * commit() function of the transaction.
   * TODO(discuss) should I make this private and friend class Transaction? Then
   * again, the user of this class shouldn't be anyone else but TableInterface
   * and Transaction.
   */
  bool submit(const Hash& rowId, const Revision& revision);
};

} /* namespace map_api */

#endif /* HISTORY_H_ */
