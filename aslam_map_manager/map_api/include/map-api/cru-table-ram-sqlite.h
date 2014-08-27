#ifndef MAP_API_CRU_TABLE_RAM_SQLITE_H_
#define MAP_API_CRU_TABLE_RAM_SQLITE_H_

#include <string>

#include "map-api/cru-table.h"
#include "map-api/sqlite-interface.h"

namespace map_api {

class CRUTableRamSqlite : public CRUTable {
 public:
  virtual ~CRUTableRamSqlite();

 private:
  virtual bool initCRUDerived() final override;
  virtual bool insertCRUDerived(Revision* query) final override;
  virtual bool bulkInsertCRUDerived(const RevisionMap& query) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual int findByRevisionCRUDerived(const std::string& key,
                                       const Revision& valueHolder,
                                       const LogicalTime& time,
                                       RevisionMap* dest) final override;
  virtual int countByRevisionCRUDerived(const std::string& key,
                                        const Revision& valueHolder,
                                        const LogicalTime& time) final override;

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual void findHistoryByRevisionCRUDerived(const std::string& key,
                                               const Revision& valueHolder,
                                               const LogicalTime& time,
                                               HistoryMap* dest) final override;

  SqliteInterface sqlite_interface_;
};

}  // namespace map_api

#endif  // MAP_API_CRU_TABLE_RAM_SQLITE_H_
