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
  virtual bool rawInsertQuery(Revision& query) const override;
  /**
   * Extension to CR interface: Get latest version at given time.
   */
  std::shared_ptr<Revision> rawGetRowAtTime(
      const Id& id, const Time& time) const;
  /**
   * Dump table according to state at given time TODO(tcies) also in CR
   */
  void rawDumpAtTime(const Time& time,
                     std::vector<std::shared_ptr<Revision> >* dest) const;
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match.
   */
  bool rawUpdateQuery(Revision& query) const;
  bool rawLatestUpdateTime(const Id& id, Time* time) const;

 private:
  /**
   * Unique ptr for history needs to be initialized with the table name.
   */
  std::unique_ptr<History> history_;
};

}

#endif  // TABLE_INTERFACE_H
