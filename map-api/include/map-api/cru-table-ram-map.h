#ifndef MAP_API_CRU_TABLE_RAM_MAP_H_
#define MAP_API_CRU_TABLE_RAM_MAP_H_

#include <list>
#include <string>
#include <vector>

#include <map-api/cru-table.h>

namespace map_api {

class CRUTableRamMap : public CRUTable {
 public:
  virtual ~CRUTableRamMap();

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
                                  RevisionMap* dest) const final override;
  virtual void findByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time,
                                       RevisionMap* dest) const final override;
  virtual int countByRevisionCRDerived(
      int key, const Revision& valueHolder,
      const LogicalTime& time) const final override;
  virtual void getAvailableIdsCRDerived(const LogicalTime& time,
      std::vector<Id>* ids) const final override;
  virtual int countByChunkCRDerived(
      const Id& chunk_id, const LogicalTime& time) const final override;

  virtual bool insertUpdatedCRUDerived(
      const std::shared_ptr<Revision>& query) final override;
  virtual void findHistoryByRevisionCRUDerived(
      int key, const Revision& valueHolder, const LogicalTime& time,
      HistoryMap* dest) const final override;
  virtual void chunkHistory(const Id& chunk_id, const LogicalTime& time,
                            HistoryMap* dest) const final override;
  virtual void itemHistoryCRUDerived(const Id& id, const LogicalTime& time,
                                     History* dest) const final override;

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder, const LogicalTime& time,
      const std::function<void(
          const Id& id, const Revision& item)>& action) const;
  inline void forChunkItemsAtTime(
      const Id& chunk_id, const LogicalTime& time,
      const std::function<void(
          const Id& id, const Revision& item)>& action) const;
  inline void trimToTime(const LogicalTime& time, HistoryMap* subject) const;

  HistoryMap data_;
};

} /* namespace map_api */

#endif  // MAP_API_CRU_TABLE_RAM_MAP_H_
