#ifndef CRU_TABLE_RAM_CACHE_H_
#define CRU_TABLE_RAM_CACHE_H_

#include "map-api/cru-table.h"
#include "map-api/sqlite-interface.h"

namespace map_api {

class CRUTableRAMCache : public CRUTable {
 public:
  virtual ~CRUTableRAMCache();
 private:
  virtual bool initCRUDerived() final override;
  virtual bool insertCRUDerived(Revision* query) final override;
  virtual bool patchCRDerived(const Revision& query) final override;
  virtual int findByRevisionCRUDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) final override;

  virtual bool insertUpdatedCRUDerived(const Revision& query) final override;
  virtual bool updateCurrentReferToUpdatedCRUDerived(
      const Id& id, const Time& current_time, const Time& updated_time)
  final override;

  SqliteInterface sqlite_interface_;
};

} /* namespace map_api */

#endif /* CRU_TABLE_RAM_CACHE_H_ */
