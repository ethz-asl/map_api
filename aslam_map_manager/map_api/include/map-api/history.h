#ifndef HISTORY_H_
#define HISTORY_H_

#include <map-api/cr-table-interface.h>
#include <map-api/time.h>

namespace map_api {

class History final : public CRTableInterface {
 public:
  /**
   * Define the table of which the history is to be kept.
   * Will create a table with the name <tableName>_history.
   */
  explicit History(const std::string& tableName);
  /**
   * Takes the table name taken from constructor to set up table interface
   */
  virtual inline const std::string tableName() override;
  /**
   * Prepare a history item for insertion by transaction.
   */
  std::shared_ptr<Revision> prepareForInsert(const Revision& revision,
                                             const Id& previous) const;

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
  std::shared_ptr<Revision> revisionAt(const Id& id,
                                       const Time& time);

  std::string tableName_;
};

} /* namespace map_api */

#endif /* HISTORY_H_ */
