#ifndef MAP_API_CR_TABLE_H_
#define MAP_API_CR_TABLE_H_

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
class ConstRevisionMap;
class MutableRevisionMap;
class Revision;

/**
 * Abstract table class. Implements structure definition, provides a non-virtual
 * interface for table operations.
 */
class CRTable {
 public:
  enum class Type {
    CR,
    CRU
  };

  virtual ~CRTable();

  /**
   * Initializes the table structure from passed table descriptor.
   * TODO(tcies) private, friend TableManager
   */
  virtual bool init(std::unique_ptr<TableDescriptor>* descriptor)
  final;

  bool isInitialized() const;
  /**
   * Returns the table name as specified in the descriptor.
   */
  const std::string& name() const;
  /**
   * Returns an empty revision having the structure as defined in the
   * descriptor
   */
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
  /**
   * Pointer to query, as it is modified according to the default field policies
   * of the respective implementation. This implementation wrapper checks table
   * and query for sanity before calling the implementation:
   * - Table initialized?
   * - Do query and table structure match?
   * - Are the default fields set (or sets them accordingly, see
   *   ensureDefaultFields() and ensureDefaultFieldsCRDerived())
   *
   * The bulk flavor is for bundling multiple inserts into one transaction,
   * for performance reasons. It also allows specifying the time of insertion,
   * for singular transaction commit times.
   * TODO(tcies) make void where possible
   */
  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& query) final;
  virtual bool bulkInsert(const MutableRevisionMap& query) final;
  virtual bool bulkInsert(const MutableRevisionMap& query,
                          const LogicalTime& time) final;
  /**
   * Unlike insert, patch does not modify the query, but assumes that all
   * default values are set correctly.
   */
  virtual bool patch(const std::shared_ptr<Revision>& revision) final;
  /**
   * Returns revision of item that has been current at "time" or an invalid
   * pointer if the item hasn't been inserted at "time"
   */
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          const LogicalTime& time) const;

  template <typename IdType>
  void getAvailableIds(const LogicalTime& time,
                       std::vector<IdType>* ids) const;
  /**
   * Puts all items that match key = value at time into dest and returns the
   * amount of items in dest.
   * If "key" is -1, no filter will be applied
   */
  template <typename ValueType>
  void find(int key, const ValueType& value, const LogicalTime& time,
            ConstRevisionMap* dest) const;
  void dumpChunk(const common::Id& chunk_id, const LogicalTime& time,
                 ConstRevisionMap* dest) const;

  /**
   * Same as find() but not typed. Value is looked up in the corresponding field
   * of valueHolder.
   */
  virtual void findByRevision(int key, const Revision& valueHolder,
                              const LogicalTime& time,
                              ConstRevisionMap* dest) const final;
  /**
   * Same as find() but makes the assumption that there is only one result.
   */
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(
      int key, const ValueType& value, const LogicalTime& time) const;

  /**
   * Same as count() but not typed. Value is looked up in the corresponding
   * field
   * of valueHolder.
   */
  virtual int countByRevision(int key, const Revision& valueHolder,
                              const LogicalTime& time) const final;

  virtual void dump(const LogicalTime& time, ConstRevisionMap* dest) const
      final;

  /**
   * Count all items that match key = value at time.
   * If "key" is an empty string, no filter will be applied.
   */
  template <typename ValueType>
  int count(int key, const ValueType& value, const LogicalTime& time) const;
  int countByChunk(const common::Id& id, const LogicalTime& time) const;

  void clear();

  /**
   * The following struct can be used to automatically supply table name and
   * item id to a glog message.
   */
  typedef struct ItemDebugInfo {
    std::string table;
    std::string id;
    ItemDebugInfo(const std::string& _table, const common::Id& _id) :
      table(_table), id(_id.hexString()) {}
  } ItemDebugInfo;

  virtual Type type() const;

 protected:
  std::unique_ptr<TableDescriptor> descriptor_;

 private:
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  /**
   * Do here whatever is specific to initializing the derived type
   */
  virtual bool initCRDerived() = 0;
  virtual bool insertCRDerived(const LogicalTime& time,
                               const std::shared_ptr<Revision>& query) = 0;
  virtual bool bulkInsertCRDerived(const MutableRevisionMap& query,
                                   const LogicalTime& time) = 0;
  virtual bool patchCRDerived(const std::shared_ptr<Revision>& query) = 0;
  virtual std::shared_ptr<const Revision> getByIdCRDerived(
      const common::Id& id, const LogicalTime& time) const = 0;
  virtual void dumpChunkCRDerived(const common::Id& chunk_id,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const = 0;
  /**
   * If key is -1, this should return all the data in the table.
   */
  virtual void findByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time,
                                       ConstRevisionMap* dest) const = 0;
  virtual void getAvailableIdsCRDerived(const LogicalTime& time,
                                        std::vector<common::Id>* ids) const = 0;

  /**
   * If key is -1, this should return all the data in the table.
   */
  virtual int countByRevisionCRDerived(int key, const Revision& valueHolder,
                                       const LogicalTime& time) const = 0;
  virtual int countByChunkCRDerived(const common::Id& chunk_id,
                                    const LogicalTime& time) const = 0;

  virtual void clearCRDerived() = 0;

  bool initialized_ = false;

  mutable std::mutex access_mutex_;
};

std::ostream& operator<< (std::ostream& stream, const
                          CRTable::ItemDebugInfo& info);

}  // namespace map_api

#include "./cr-table-inl.h"

#endif  // MAP_API_CR_TABLE_H_
