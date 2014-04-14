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
 public:
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

 private:
  /**
   * History table fields:
   * - ID (implicit)
   * - rowID: Hash Identifier of the row that the given revision is a revision
   * of TODO(tcies) isn't this redundant?
   * - previous: A history item always refers to its previous revision.
   * - revision: serialized protobuf revision of the updated state
   * - time: time of revision
   */
  virtual bool define();
  /**
   * The following part is managed by transactions only.
   */
  friend class Transaction;
  /**
   * Returns shared pointer of latest revision at requested time. Supplied id is
   * reference to any revision. If that revision is older than the supplied
   * time, it is the one that is returned.
   */
  std::shared_ptr<Revision> revisionAt(const Hash& id,
                                       const Time& time);
  /**
   * Inserts revision into history. Proper linking is responsibility of
   * Transaction.
   */
  Hash rawInsert(Revision& revision, const Hash& previous);

  const CRUTableInterface& table_;
};

} /* namespace map_api */

#endif /* HISTORY_H_ */
