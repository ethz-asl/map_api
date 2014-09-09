#ifndef MAP_API_CRU_TABLE_RAM_MAP_H_
#define MAP_API_CRU_TABLE_RAM_MAP_H_

#include <list>
#include <string>

#include "map-api/cru-table.h"

namespace map_api {

class CRUTableRamMap : public CRUTable {
 public:
  virtual ~CRUTableRamMap();

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
  virtual void getAvailableIdsCRDerived(
      const LogicalTime& time, std::unordered_set<Id>* ids) final override;

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual void findHistoryByRevisionCRUDerived(const std::string& key,
                                               const Revision& valueHolder,
                                               const LogicalTime& time,
                                               HistoryMap* dest) final override;

  HistoryMap data_;
};

} /* namespace map_api */

#endif  // MAP_API_CRU_TABLE_RAM_MAP_H_
