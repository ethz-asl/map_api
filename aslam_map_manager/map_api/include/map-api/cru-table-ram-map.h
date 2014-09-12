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
  virtual bool initCRDerived() final override;
  virtual bool insertCRUDerived(Revision* query) final override;
  virtual bool bulkInsertCRUDerived(const RevisionMap& query) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual std::shared_ptr<Revision> getByIdCRDerived(
      const Id& id, const LogicalTime& time) const final override;
  virtual int findByRevisionCRDerived(int key, const Revision& valueHolder,
                                      const LogicalTime& time,
                                      RevisionMap* dest) final override;
  virtual int countByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time) final override;
  virtual void getAvailableIdsCRDerived(
      const LogicalTime& time, std::unordered_set<Id>* ids) final override;

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual void findHistoryByRevisionCRUDerived(int key,
                                               const Revision& valueHolder,
                                               const LogicalTime& time,
                                               HistoryMap* dest) final override;

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder, const LogicalTime& time,
      const std::function<
          void(const Id& id, const History::const_iterator& item)>& action);

  HistoryMap data_;
};

} /* namespace map_api */

#endif  // MAP_API_CRU_TABLE_RAM_MAP_H_
