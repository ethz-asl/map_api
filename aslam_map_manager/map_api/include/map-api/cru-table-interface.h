#ifndef TABLE_INTERFACE_H
#define TABLE_INTERFACE_H

#include <vector>
#include <memory>
#include <map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/cr-table-interface.h"
#include "map-api/history.h"
#include "map-api/revision.h"
#include "map-api/time.h"
#include "core.pb.h"

namespace map_api {

/**
 * Provides interface to map api tables.
 */
class CRUTableInterface : public CRTableInterface{
 public:
  virtual bool init();

  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  /**
   * This table name will appear in the database, so it must be chosen SQL
   * friendly: Letters and underscores only.
   */
  virtual const std::string name() const = 0;
  /**
   * Function to be implemented by derivations: Define table by repeated
   * calls to addField()
   */
  virtual void define() = 0;
  virtual ~CRUTableInterface();

 protected:
  /**
   * The following functions are to be used by transactions only. They pose a
   * very crude access straight to the database, without synchronization
   * and conflict checking - that is assumed to be done by the transaction.
   */
  friend class Transaction;
  /**
   * Insertion differs from CRTableInterface in that additional default field
   * "previous" needs to be set.
   */
  virtual bool rawInsertImpl(Revision& query) const override;
  /**
   * For now, may find only by values that don't get updated (that is, it
   * looks up value in the current version of the table, then looks back to the
   * create time and verifies that the element was present at the specified
   * time). Otherwise, would
   * need to search through entire history, which is not just painful to
   * implement, but would also probably not work well on a distributed system.
   * TODO(discuss) right?
   * TODO(tcies) if yes, formalize and embed in code
   */
  virtual int rawFindByRevisionImpl(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::vector<std::shared_ptr<Revision> >* dest)  const;
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match.
   */
  bool rawUpdate(Revision& query) const;
  virtual bool rawUpdateImpl(Revision& query) const;
  bool rawLatestUpdateTime(const Id& id, Time* time) const;

 private:
  /**
   * Unique ptr for history needs to be initialized with the table name.
   */
  std::unique_ptr<History> history_;
};

}

#endif  // TABLE_INTERFACE_H
