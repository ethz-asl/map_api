#ifndef MAP_API_CRU_TABLE_RAM_CACHE_H_
#define MAP_API_CRU_TABLE_RAM_CACHE_H_

#include <string>

#include "map-api/cru-table.h"
#include "map-api/sqlite-interface.h"

namespace map_api {

class CRUTableRAMCache : public CRUTable {
 public:
  virtual ~CRUTableRAMCache();
 private:
  virtual bool initCRUDerived() final override;
  virtual bool insertCRUDerived(Revision* query) final override;
  virtual bool bulkInsertCRUDerived(const RevisionMap& query) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual int findByRevisionCRUDerived(
      const std::string& key, const Revision& valueHolder,
      const LogicalTime& time, RevisionMap* dest) final override;
  virtual int countByRevisionCRUDerived(const std::string& key,
                                        const Revision& valueHolder,
                                        const LogicalTime& time) final override;

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual bool updateCurrentReferToUpdatedCRUDerived(
      const Id& id, const LogicalTime& current_time,
      const LogicalTime& updated_time) final override;

  SqliteInterface sqlite_interface_;
};

}  // namespace map_api

#endif  // MAP_API_CRU_TABLE_RAM_CACHE_H_
