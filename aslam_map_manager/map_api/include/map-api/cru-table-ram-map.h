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

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual bool updateCurrentReferToUpdatedCRUDerived(
      const Id& id, const LogicalTime& current_time,
      const LogicalTime& updated_time) final override;

  class HistoryType : public std::list<Revision> {
   public:
    const_iterator latestAt(const LogicalTime& time) const;
    /**
     * Index_guess guesses the position of the update time field in the Revision
     * proto.
     */
    const_iterator latestAt(const LogicalTime& time, int index_guess) const;
  };
  // Latest at front
  typedef std::unordered_map<Id, HistoryType> HistoryMapType;

  HistoryMapType data_;
};

} /* namespace map_api */

#endif  // MAP_API_CRU_TABLE_RAM_MAP_H_
