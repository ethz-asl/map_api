#ifndef MAP_API_CRU_TABLE_H_
#define MAP_API_CRU_TABLE_H_

#include <vector>
#include <memory>
#include <map>
#include <string>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/logical-time.h"
#include "./core.pb.h"

DECLARE_bool(cru_linked);

namespace map_api {

/**
 * Provides interface to map api tables.
 */
class CRUTable : public CRTable {
 public:
  virtual ~CRUTable();
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match. Query may be modified according to the default
   * field policy.
   * Calls insertUpdatedCRUDerived and updateCurrentReferToUpdatedCRUDerived.
   * It is possible to specify update time for singular times of transactions.
   * TODO(tcies) make it the only possible way of setting time
   */
  bool update(Revision* query);  // TODO(tcies) void
  void update(Revision* query, const LogicalTime& time);
  bool getLatestUpdateTime(const Id& id, LogicalTime* time);

  virtual Type type() const final override;

  /**
   * Default fields for internal management,
   */
  static const std::string kUpdateTimeField;
  static const std::string kPreviousTimeField;  // time of previous revision
  static const std::string kNextTimeField;      // time of next revision

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(Revision* query) final override;
  virtual bool bulkInsertCRDerived(const RevisionMap& query) final override;
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder,
      const LogicalTime& time, CRTable::RevisionMap* dest) final override;
  virtual int countByRevisionCRDerived(const std::string& key,
                                       const Revision& valueHolder,
                                       const LogicalTime& time) final override;
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   * The CRTable class contains most documentation on these functions.
   */
  virtual bool initCRUDerived() = 0;
  virtual bool insertCRUDerived(Revision* query) = 0;
  virtual bool bulkInsertCRUDerived(const RevisionMap& query) = 0;
  virtual bool patchCRDerived(const Revision& query) override = 0;
  virtual int findByRevisionCRUDerived(
      const std::string& key, const Revision& valueHolder,
      const LogicalTime& time, CRTable::RevisionMap* dest) = 0;
  virtual int countByRevisionCRUDerived(const std::string& key,
                                        const Revision& valueHolder,
                                        const LogicalTime& time) = 0;

  /**
   * Implement insertion of the updated revision
   */
  virtual bool insertUpdatedCRUDerived(const Revision& query) = 0;
  /**
   * Implement the maintenance of each revision referring to the next revision
   * by setting kNextTimeField of (id, current_time) to updated_time
   */
  virtual bool updateCurrentReferToUpdatedCRUDerived(
      const Id& id, const LogicalTime& current_time,
      const LogicalTime& updated_time) = 0;
  friend class Chunk;
};

}  // namespace map_api

#endif  // MAP_API_CRU_TABLE_H_
