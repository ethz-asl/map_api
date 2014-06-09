#ifndef MAP_API_CR_TABLE_RAM_CACHE_H_
#define MAP_API_CR_TABLE_RAM_CACHE_H_

#include "map-api/cr-table.h"
#include "map-api/sqlite-interface.h"

namespace map_api {

/**
 * TODO(tcies) make this a proper RAM cache (maps, not RAM sqlite) and create
 * a CRTableDiskCache for disk SQLite
 */
class CRTableRAMCache final : public CRTable {
 public:
  virtual ~CRTableRAMCache();
 private:
  virtual bool initCRDerived() override;
  virtual void defineDefaultFieldsCRDerived() override;
  virtual void ensureDefaulFieldsCRDerived(Revision* query) const override;
  virtual bool insertCRDerived(Revision* query) override;
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) override;
 private:
  SqliteInterface sqlite_interface_;
};

} /* namespace map_api */

#endif /* MAP_API_CR_TABLE_RAM_CACHE_H_ */
