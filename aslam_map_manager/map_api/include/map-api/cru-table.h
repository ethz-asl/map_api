#ifndef MAP_API_CRU_TABLE_H_
#define MAP_API_CRU_TABLE_H_

#include <vector>
#include <memory>
#include <map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/time.h"
#include "core.pb.h"

namespace map_api {

/**
 * Provides interface to map api tables.
 */
class CRUTable : public CRTable {
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
  virtual ~CRUTable();

 protected:
  /**
   * Default table fields
   */
  static const std::string kUpdateTimeField;
  static const std::string kPreviousTimeField; // time of previous revision
  static const std::string kNextTimeField; // time of next revision
  /**
   * The following functions are to be used by transactions only. They pose a
   * very crude access straight to the database, without synchronization
   * and conflict checking - that is assumed to be done by the transaction.
   */
  friend class LocalTransaction;

  virtual bool rawInsertImpl(Revision& query) const override;

  virtual int rawFindByRevisionImpl(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest)  const
  override;
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match.
   */
  bool rawUpdate(Revision& query) const;
  virtual bool rawUpdateImpl(Revision& query) const;
  bool rawLatestUpdateTime(const Id& id, Time* time) const;
};

}

#endif  // MAP_API_CRU_TABLE_H_
