#ifndef MAP_API_CR_TABLE_RAM_MAP_H_
#define MAP_API_CR_TABLE_RAM_MAP_H_

#include <string>

#include "map-api/cr-table.h"

namespace map_api {

class CRTableRamMap : public CRTable {
 public:
  virtual ~CRTableRamMap();

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(const LogicalTime& time,
                               Revision* query) final override;
  virtual bool bulkInsertCRDerived(const RevisionMap& query,
                                   const LogicalTime& time) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual int findByRevisionCRDerived(
      int key, const Revision& valueHolder, const LogicalTime& time,
      CRTable::RevisionMap* dest) final override;
  virtual std::shared_ptr<Revision> getByIdCRDerived(
      const Id& id, const LogicalTime& time) const final override;
  virtual void getAvailableIdsCRDerived(
      const LogicalTime& time, std::unordered_set<Id>* ids) final override;
  virtual int countByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time) final override;

  typedef std::unordered_map<Id, Revision> MapType;
  MapType data_;
};

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_RAM_MAP_H_
