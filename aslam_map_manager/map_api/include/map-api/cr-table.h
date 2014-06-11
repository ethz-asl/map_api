#ifndef MAP_API_CR_TABLE_H_
#define MAP_API_CR_TABLE_H_

#include <vector>
#include <memory>
#include <map>
#include <unordered_map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/id.h"
#include "map-api/table-descriptor.h"
#include "map-api/revision.h"
#include "core.pb.h"

namespace map_api {

/**
 * Abstract table class. Implements structure definition, provides a non-virtual
 * interface for table operations.
 */
class CRTable {
 public:
  /**
   * Default fields
   */
  static const std::string kIdField;
  static const std::string kInsertTimeField;

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
   */
  virtual bool insert(Revision* query) final;

  virtual std::shared_ptr<Revision> getById(
      const Id& id, const Time& time);
  /**
   * Puts all items that match key = value at time into dest and returns the
   * amount of items in dest.
   * If "key" is an empty string, no filter will be applied (equivalent to
   * dump())
   */
  template<typename ValueType>
  int find(const std::string& key, const ValueType& value, const Time& time,
           std::unordered_map<Id, std::shared_ptr<Revision> >* dest);
  /**
   * Same as find() but not typed. Value is looked up in the corresponding field
   * of valueHolder.
   */
  virtual int findByRevision(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) final;
  /**
   * Same as find() but makes the assumption that there is only one result.
   */
  template<typename ValueType>
  std::shared_ptr<Revision> findUnique(
      const std::string& key, const ValueType& value, const Time& time);
  virtual void dump(const Time& time,
                    std::unordered_map<Id, std::shared_ptr<Revision> >* dest)
  final;

  /**
   * The following struct can be used to automatically supply table name and
   * item id to a glog message.
   */
  typedef struct ItemDebugInfo{
    std::string table;
    std::string id;
    ItemDebugInfo(const std::string& _table, const Id& _id) :
      table(_table), id(_id.hexString()) {}
  } ItemDebugInfo;

 private:
  /**
   * Defines default table fields.
   */
  virtual void defineDefaultFields() final;
  /**
   * Ensures the default fields are properly set
   */
  virtual void ensureDefaultFields(Revision* query) const final;

  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  /**
   * Do here whatever is specific to initializing the derived type
   */
  virtual bool initCRDerived() = 0;
  /**
   * Define default fields by repeated calls to descriptor_.addField().
   */
  virtual void defineDefaultFieldsCRDerived() = 0;
  /**
   * Ensure fields set with defineDefaultFieldsCRDerived are properly set in the
   * passed query.
   */
  virtual void ensureDefaulFieldsCRDerived(Revision* query) const = 0;
  /**
   * Minimal required table operations
   */
  virtual bool insertCRDerived(Revision* query) = 0;
  /**
   * If key is an empty string, this should return all the data in the table.
   */
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) = 0;

  std::unique_ptr<TableDescriptor> descriptor_;
  bool initialized_ = false;
};

std::ostream& operator<< (std::ostream& stream, const
                          CRTable::ItemDebugInfo& info);

} /* namespace map_api */

#include "map-api/cr-table-inl.h"

#endif /* MAP_API_CR_TABLE_H_ */
