#ifndef MAP_API_CRU_TABLE_STXXL_MAP_H_
#define MAP_API_CRU_TABLE_STXXL_MAP_H_

#include <list>
#include <string>

#include "map-api/cru-table.h"
#include "map-api/stxxl-revision-store.h"

namespace map_api {

class CRUTableSTXXLMap : public CRUTable {
 public:
  virtual ~CRUTableSTXXLMap();

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRUDerived(
      const std::shared_ptr<Revision>& query) final override;
  virtual bool bulkInsertCRUDerived(
      const NonConstRevisionMap& query) final override;
  virtual bool patchCRDerived(
      const std::shared_ptr<Revision>& query) final override;
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
      std::unordered_set<Id>* ids) const final override;
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
      const std::function<
          void(const Id& id, const Revision& item)>& action) const;
  inline void forChunkItemsAtTime(
      const Id& chunk_id, const LogicalTime& time,
      const std::function<
          void(const Id& id, const Revision& item)>& action) const;
  inline void trimToTime(const LogicalTime& time, HistoryMap* subject) const;

  class STXXLHistory : public std::list<RevisionInformation> {
   public:
    inline const_iterator latestAt(const LogicalTime& time) const {
      for (const_iterator it = cbegin(); it != cend(); ++it) {
        if (it->update_time_ <= time) {
          return it;
        }
      }
      return cend();
    }
  };
  typedef std::unordered_map<Id, STXXLHistory> STXXLHistoryMap;
  STXXLHistoryMap data_;

  static constexpr int kBlockSize = 64;
  STXXLRevisionStore<kBlockSize> revision_store_;
};

} /* namespace map_api */

#endif  // MAP_API_CRU_TABLE_STXXL_MAP_H_
