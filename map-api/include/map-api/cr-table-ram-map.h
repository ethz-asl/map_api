#ifndef MAP_API_CR_TABLE_RAM_MAP_H_
#define MAP_API_CR_TABLE_RAM_MAP_H_

#include <vector>

#include <map-api/cr-table.h>

namespace map_api {

class CRTableRamMap : public CRTable {
 public:
  virtual ~CRTableRamMap();

 private:
  virtual bool initCRDerived() final override;
  virtual bool insertCRDerived(const LogicalTime& time,
                               const std::shared_ptr<Revision>& query)
      final override;
  virtual bool bulkInsertCRDerived(const MutableRevisionMap& query,
                                   const LogicalTime& time) final override;
  virtual bool patchCRDerived(const std::shared_ptr<Revision>& query)
      final override;
  virtual void dumpChunkCRDerived(const common::Id& chunk_id,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const final override;
  virtual void findByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time,
                                       ConstRevisionMap* dest) const
      final override;
  virtual std::shared_ptr<const Revision> getByIdCRDerived(
      const common::Id& id, const LogicalTime& time) const final override;
  virtual void getAvailableIdsCRDerived(const LogicalTime& time,
      std::vector<common::Id>* ids) const final override;
  virtual int countByRevisionCRDerived(
      int key, const Revision& valueHolder,
      const LogicalTime& time) const final override;
  virtual int countByChunkCRDerived(
      const common::Id& chunk_id, const LogicalTime& time) const final override;
  virtual void clearCRDerived() final override;

  typedef std::unordered_map<common::Id,
      std::shared_ptr<const Revision> > MapType;
  MapType data_;
};

}  // namespace map_api

#endif  // MAP_API_CR_TABLE_RAM_MAP_H_
