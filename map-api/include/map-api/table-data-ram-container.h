#ifndef MAP_API_TABLE_DATA_RAM_CONTAINER_H_
#define MAP_API_TABLE_DATA_RAM_CONTAINER_H_

#include <vector>

#include "map-api/table-data-container-base.h"

namespace map_api {

class TableDataRamContainer : public TableDataContainerBase {
 public:
  virtual ~TableDataRamContainer();

 private:
  virtual bool initImpl() final override;
  virtual bool insertImpl(const std::shared_ptr<const Revision>& query)
      final override;
  virtual bool bulkInsertImpl(const MutableRevisionMap& query) final override;
  virtual bool patchImpl(const std::shared_ptr<const Revision>& query)
      final override;
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const common::Id& id, const LogicalTime& time) const final override;
  virtual void dumpChunkImpl(const common::Id& chunk_id,
                             const LogicalTime& time,
                             ConstRevisionMap* dest) const final override;
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const final override;
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const final override;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<common::Id>* ids) const
      final override;
  virtual int countByChunkImpl(const common::Id& chunk_id,
                               const LogicalTime& time) const final override;

  virtual bool insertUpdatedImpl(const std::shared_ptr<Revision>& query)
      final override;
  virtual void findHistoryByRevisionImpl(int key, const Revision& valueHolder,
                                         const LogicalTime& time,
                                         HistoryMap* dest) const final override;
  virtual void chunkHistory(const common::Id& chunk_id, const LogicalTime& time,
                            HistoryMap* dest) const final override;
  virtual void itemHistoryImpl(const common::Id& id, const LogicalTime& time,
                               History* dest) const final override;
  virtual void clearImpl() final override;

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder, const LogicalTime& time,
      const std::function<void(const common::Id& id, const Revision& item)>&
          action) const;
  inline void forChunkItemsAtTime(
      const common::Id& chunk_id, const LogicalTime& time,
      const std::function<void(const common::Id& id, const Revision& item)>&
          action) const;
  inline void trimToTime(const LogicalTime& time, HistoryMap* subject) const;

  HistoryMap data_;
};

} /* namespace map_api */

#endif  // MAP_API_TABLE_DATA_RAM_CONTAINER_H_
