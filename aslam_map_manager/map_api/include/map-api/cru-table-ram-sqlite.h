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
  virtual bool initCRDerived() final override;
  virtual bool insertCRUDerived(const std::shared_ptr<Revision>& query)
      final override;
  virtual bool bulkInsertCRUDerived(const NonConstRevisionMap& query)
      final override;
  virtual bool patchCRDerived(const std::shared_ptr<Revision>& query)
      final override;
  virtual std::shared_ptr<const Revision> getByIdCRDerived(
      const Id& id, const LogicalTime& time) const final override;
  virtual void dumpChunkCRDerived(const Id& chunk_id, const LogicalTime& time,
                                  RevisionMap* dest) final override;
  virtual void findByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time,
                                       RevisionMap* dest) final override;
  virtual void getAvailableIdsCRDerived(
      const LogicalTime& time, std::unordered_set<Id>* ids) final override;
  virtual int countByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time) final override;
  virtual int countByChunkCRDerived(const Id& chunk_id,
                                    const LogicalTime& time) final override;

  virtual bool insertUpdatedCRUDerived(const std::shared_ptr<Revision>& query)
      final override;
  virtual void findHistoryByRevisionCRUDerived(int key,
                                               const Revision& valueHolder,
                                               const LogicalTime& time,
                                               HistoryMap* dest) final override;
  virtual void chunkHistory(const Id& chunk_id, const LogicalTime& time,
                            HistoryMap* dest) final override;
  virtual void itemHistoryCRUDerived(const Id& id, const LogicalTime& time,
                                     History* dest) final override;

  SqliteInterface sqlite_interface_;
};

}  // namespace map_api

#endif  // MAP_API_CRU_TABLE_RAM_SQLITE_H_
