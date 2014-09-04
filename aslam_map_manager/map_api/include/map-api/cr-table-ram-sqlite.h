#ifndef MAP_API_CR_TABLE_RAM_SQLITE_H_
#define MAP_API_CR_TABLE_RAM_SQLITE_H_

#include <string>

#include "map-api/cr-table.h"
#include "map-api/sqlite-interface.h"

namespace map_api {

class CRTableRamSqlite final : public CRTable {
 public:
  virtual ~CRTableRamSqlite();

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(Revision* query) final override;
  virtual bool bulkInsertCRDerived(const RevisionMap& query,
                                   const LogicalTime& time) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder,
      const LogicalTime& time, CRTable::RevisionMap* dest) final override;
  virtual void getAvailableIdsCRDerived(
      const LogicalTime& time, std::unordered_set<Id>* ids) final override;
  virtual int countByRevisionCRDerived(const std::string& key,
                                       const Revision& valueHolder,
                                       const LogicalTime& time) final override;

  SqliteInterface sqlite_interface_;
};

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_RAM_SQLITE_H_
