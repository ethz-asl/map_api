#ifndef MAP_API_CHUNK_DATA_CONTAINER_BASE_H_
#define MAP_API_CHUNK_DATA_CONTAINER_BASE_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "map-api/table-descriptor.h"
#include "./core.pb.h"

namespace common {
class Id;
}  // namespace common

namespace map_api {
class LegacyChunk;
class ConstRevisionMap;
class MutableRevisionMap;
class Revision;

class ChunkDataContainerBase {
  friend class LegacyChunk;

 public:
  virtual ~ChunkDataContainerBase();
  ChunkDataContainerBase();

  bool init(std::shared_ptr<TableDescriptor> descriptor);
  bool isInitialized() const;
  const std::string& name() const;
  std::shared_ptr<Revision> getTemplate() const;

  // ====
  // READ
  // ====
  /**
   * Returns revision of item that has been current at "time" or an invalid
   * pointer if the item hasn't been inserted at "time"
   */
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          const LogicalTime& time) const;
  // If "key" is -1, no filter will be applied
  template <typename ValueType>
  void find(int key, const ValueType& value, const LogicalTime& time,
            ConstRevisionMap* dest) const;
  void dump(const LogicalTime& time, ConstRevisionMap* dest) const;
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(int key, const ValueType& value,
                                             const LogicalTime& time) const;
  void findByRevision(int key, const Revision& valueHolder,
                      const LogicalTime& time, ConstRevisionMap* dest) const;

  // ====
  // MISC
  // ====
  template <typename IdType>
  void getAvailableIds(const LogicalTime& time, std::vector<IdType>* ids) const;
  int numAvailableIds(const LogicalTime& time) const;
  // If "key" is -1, no filter will be applied.
  template <typename ValueType>
  int count(int key, const ValueType& value, const LogicalTime& time) const;
  virtual int countByRevision(int key, const Revision& valueHolder,
                              const LogicalTime& time) const final;
  bool getLatestUpdateTime(const common::Id& id, LogicalTime* time);
  struct ItemDebugInfo {
    std::string table;
    std::string id;
    ItemDebugInfo(const std::string& _table, const common::Id& _id)
    : table(_table), id(_id.hexString()) {}
  };

 protected:
  mutable std::mutex access_mutex_;
  std::shared_ptr<TableDescriptor> descriptor_;

 private:
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  // Do here whatever is specific to initializing the derived type
  virtual bool initImpl() = 0;
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const common::Id& id, const LogicalTime& time) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const = 0;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<common::Id>* ids) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const = 0;

  bool initialized_;
};

std::ostream& operator<<(std::ostream& stream,
                         const ChunkDataContainerBase::ItemDebugInfo& info);

}  // namespace map_api

#include "./chunk-data-container-base-inl.h"

#endif  // MAP_API_CHUNK_DATA_CONTAINER_BASE_H_
