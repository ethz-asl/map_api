/*
 * history.h
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#ifndef HISTORY_H_
#define HISTORY_H_

#include <map-api/cru-table-interface.h>
#include <map-api/time.h>

namespace map_api {

class History : public CRTableInterface {
  friend class CRUTableInterface;
 private:
  /**
   * Define the table of which the history is to be kept.
   * Will create a table with the name <table.name()>_history.
   * Owner is used later to verify that owner locks affected rows when pushing
   * a revision to the revision history
   */
  explicit History(const CRUTableInterface& table);
  /**
   * Takes the table name taken from constructor to set up table interface
   */
  virtual bool init();
  /**
   * History table fields:
   * - ID (implicit)
   * - rowID: Hash Identifier of the row that the given revision is a revision
   * of
   * - previous: A history item always refers to its previous revision.
   * - newState: serialized protobuf revision of the updated state
   *
   * TODO(discuss) updateable tables will not contain the actual revision
   * but only a reference to the latest entry in the history table, which then
   * contains the actual data
   */
  virtual bool define();
  /**
   * Returns shared pointer of revision at requested time.
   * TODO(tcies) well yes... will we need this really?
   */
  std::shared_ptr<Revision> revisionAt(const Hash& rowId,
                                       const Time& time);
  /**
   * Inserts revision into history. Proper linking is responsibility of
   * CRUTableInterface / Transaction
   */
  Hash insert(const Revision& revision, const Hash& previous);

  const CRUTableInterface& table_;
};

} /* namespace map_api */

#endif /* HISTORY_H_ */
