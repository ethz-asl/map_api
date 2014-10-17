#ifndef MAP_API_CR_TABLE_STXXL_MAP_H_
#define MAP_API_CR_TABLE_STXXL_MAP_H_

#include <string>

#include "map-api/cr-table.h"
#include "map-api/stxxl-revision-store.h"

namespace map_api {

class CRTableSTXXLMap : public CRTable {
 public:
  virtual ~CRTableSTXXLMap();

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(
      const LogicalTime& time,
      const std::shared_ptr<Revision>& query) final override;
  virtual bool bulkInsertCRDerived(const NonConstRevisionMap& query,
                                   const LogicalTime& time) final override;
  virtual bool patchCRDerived(
      const std::shared_ptr<Revision>& query) final override;
  virtual void dumpChunkCRDerived(const Id& chunk_id, const LogicalTime& time,
                                  RevisionMap* dest) const final override;
  virtual void findByRevisionCRDerived(
      int key, const Revision& valueHolder, const LogicalTime& time,
      CRTable::RevisionMap* dest) const final override;
  virtual std::shared_ptr<const Revision> getByIdCRDerived(
      const Id& id, const LogicalTime& time) const final override;
  virtual void getAvailableIdsCRDerived(const LogicalTime& time,
      std::unordered_set<Id>* ids) const final override;
  virtual int countByRevisionCRDerived(
      int key, const Revision& valueHolder,
      const LogicalTime& time) const final override;
  virtual int countByChunkCRDerived(
      const Id& chunk_id, const LogicalTime& time) const final override;

  typedef std::unordered_map<Id, RevisionInformation> MapType;
  MapType data_;
  static constexpr int kBlockSize = 64;
  STXXLRevisionStore<kBlockSize> revision_store_;
};

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_STXXL_MAP_H_
