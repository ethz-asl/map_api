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
  virtual ~CRUTable();
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match. Query may be modified according to the default
   * field policy.
   * Calls insertUpdatedCRUDerived and updateCurrentReferToUpdatedCRUDerived.
   */
  bool update(Revision* query);
  bool getLatestUpdateTime(const Id& id, Time* time);

 private:
  /**
   * Default fields for internal management,
   */
  static const std::string kUpdateTimeField;
  static const std::string kPreviousTimeField; // time of previous revision
  static const std::string kNextTimeField; // time of next revision

  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(Revision* query) final override;
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) final override;
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   * The CRTable class contains most documentation on these functions.
   */
  virtual bool initCRUDerived() = 0;
  virtual bool insertCRUDerived(Revision* query) = 0;
  virtual int findByRevisionCRUDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) = 0;

  /**
   * Implement insertion of the updated revision
   */
  virtual bool insertUpdatedCRUDerived(const Revision& query) = 0;
  /**
   * Implement the maintenance of each revision referring to the next revision
   * by setting kNextTimeField of (id, current_time) to updated_time
   */
  virtual bool updateCurrentReferToUpdatedCRUDerived(
      const Id& id, const Time& current_time, const Time& updated_time) = 0;
};

}

#endif  // MAP_API_CRU_TABLE_H_
