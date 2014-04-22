#ifndef TABLE_INTERFACE_H
#define TABLE_INTERFACE_H

#include <vector>
#include <memory>
#include <map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/cr-table-interface.h"
#include "map-api/hash.h"
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
  explicit CRUTableInterface(const Hash& owner);
  virtual bool init() = 0;
 protected:
  /**
   * Overriding CR table setup in order to implement history.
   */
  bool setup(const std::string& name);
  virtual bool define() = 0;
  /**
   * Overriding get template on order to get template of revision, not history
   * bookkeeping.
   */
  std::shared_ptr<Revision> getTemplate() const;

  /**
   * Overriding addField, as the actual data will be outsourced to the
   * history
   */
  template<typename Type>
  bool addField(const std::string& name);
  bool addField(const std::string& name,
                proto::TableFieldDescriptor_Type type);

 private:
  /**
   * This is the function that will actually add fields to this table - for
   * householding the references to the history table.
   */
  template<typename Type>
  bool addCRUField(const std::string& name);

  std::unique_ptr<History> history_;
  proto::TableDescriptor descriptor_;

  /**
   * The following functions are to be used by transactions only. They pose a
   * very crude access straight to the database, without synchronization
   * and conflict checking - that is assumed to be done by the transaction.
   */
  friend class Transaction;
  /**                                                                      U   U
   *                                                                       U   U
   * Takes hash ID and TableInsertQuery as argument and updates the row of U   U
   * the given ID with the query. IMPORTANT: This is to modify the own     U   U
   * fields of the CRU table, not the revisions in the history. That has    UUU
   * to be handled properly by the transaction itself.
   * the parameter nextRevision is the hash to the revision the CRU table item
   * is supposed to be updated to.
   */
  bool rawUpdateQuery(const Hash& id, const Hash& nextRevision);
  /**
   * Template for history bookkeeping
   */
  std::shared_ptr<Revision> getCRUTemplate() const;

  bool rawLatestUpdate(const Hash& id, Time* time) const;

};

}

#include "map-api/cru-table-interface-inl.h"

#endif  // TABLE_INTERFACE_H
