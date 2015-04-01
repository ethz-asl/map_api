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
class Chunk;
class ConstRevisionMap;
class MutableRevisionMap;
class Revision;

class ChunkDataContainerBase {
  friend class Chunk;

 public:
  virtual ~ChunkDataContainerBase();
  ChunkDataContainerBase();

  virtual bool init(std::shared_ptr<TableDescriptor> descriptor) final;
  bool isInitialized() const;
  const std::string& name() const;
  std::shared_ptr<Revision> getTemplate() const;

  /**
   * =============================================
   * "NON-VIRTUAL" INTERFACES FOR TABLE OPERATIONS
   * =============================================
   * Default behavior is implemented but can be overwritten for some functions
   * if so desired. E.g. all reading operations are based on findByRevision,
   * making this the only mandatory reading implementation by derived classes,
   * yet derived classes might optimize getById or findUnique.
   * Also, the use of "const" has been restricted to ensure flexibility of
   * derived classes.
   */

  // ======
  // CREATE
  // ======

  /**
   * Pointer to query, as it is modified according to the default field policies
   * of the respective implementation. This implementation wrapper checks table
   * and query for sanity before calling the implementation:
   * - Table initialized?
   * - Do query and table structure match?
   * - Set default fields.
   *
   * The bulk flavor is for bundling multiple inserts into one transaction,
   * for performance reasons. It also allows specifying the time of insertion,
   * for singular transaction commit times.
   * TODO(tcies) make void where possible
   */
  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& query) final;
  virtual bool bulkInsert(const LogicalTime& time,
                          const MutableRevisionMap& query) final;
  /**
   * Unlike insert, patch does not modify the query, but assumes that all
   * default values are set correctly.
   */
  virtual bool patch(const std::shared_ptr<const Revision>& revision) final;

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
  virtual void dump(const LogicalTime& time, ConstRevisionMap* dest) const
  final;
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(int key, const ValueType& value,
                                             const LogicalTime& time) const;
  virtual void findByRevision(int key, const Revision& valueHolder,
                              const LogicalTime& time,
                              ConstRevisionMap* dest) const final;
  // ============
  // READ HISTORY
  // ============
  class History : public std::list<std::shared_ptr<const Revision> > {
   public:
    virtual ~History();
    inline const_iterator latestAt(const LogicalTime& time) const;
  };
  template <typename IdType>
  void itemHistory(const IdType& id, const LogicalTime& time,
                   History* dest) const;
  typedef std::unordered_map<common::Id, History> HistoryMap;
  template <typename ValueType>
  void findHistory(int key, const ValueType& value, const LogicalTime& time,
                   HistoryMap* dest) const;
  virtual void findHistoryByRevision(int key, const Revision& valueHolder,
                                     const LogicalTime& time,
                                     HistoryMap* dest) const final;

  // ======
  // UPDATE
  // ======
  /**
   * Field ID in revision must correspond to an already present item, revision
   * structure needs to match. Query may be modified according to the default
   * field policy.
   */
  void update(const LogicalTime& time, const std::shared_ptr<Revision>& query);

  // ======
  // DELETE
  // ======
  void remove(const LogicalTime& time, const std::shared_ptr<Revision>& query);
  // avoid if possible - this is slower
  template <typename IdType>
  void remove(const LogicalTime& time, const IdType& id);
  void clear();

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

  private:
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  // Do here whatever is specific to initializing the derived type
  virtual bool initImpl() = 0;
  virtual bool insertImpl(const std::shared_ptr<const Revision>& query) = 0;
  // TODO(tcies) The best thing would be to have "query" be a ConstRevisionMap,
  // but unfortunately, that would require casting from Mutable, as the
  // revision map is mutable on the caller side.
  virtual bool bulkInsertImpl(const MutableRevisionMap& query) = 0;
  virtual bool patchImpl(const std::shared_ptr<const Revision>& query) = 0;
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const common::Id& id, const LogicalTime& time) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual void findHistoryByRevisionImpl(int key, const Revision& valueHolder,
                                         const LogicalTime& time,
                                         HistoryMap* dest) const = 0;
  virtual void chunkHistory(const common::Id& chunk_id, const LogicalTime& time,
                            HistoryMap* dest) const = 0;
  virtual void itemHistoryImpl(const common::Id& id, const LogicalTime& time,
                               History* dest) const = 0;
  virtual bool insertUpdatedImpl(const std::shared_ptr<Revision>& query) = 0;
  virtual void clearImpl() = 0;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<common::Id>* ids) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const = 0;

  bool initialized_;
  std::shared_ptr<TableDescriptor> descriptor_;
  mutable std::mutex access_mutex_;
};

std::ostream& operator<<(std::ostream& stream,
                         const ChunkDataContainerBase::ItemDebugInfo& info);

}  // namespace map_api

#include "./chunk-data-container-base-inl.h"

#endif  // MAP_API_CHUNK_DATA_CONTAINER_BASE_H_
