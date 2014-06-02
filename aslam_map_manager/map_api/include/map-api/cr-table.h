#ifndef MAP_API_CR_TABLE_H_
#define MAP_API_CR_TABLE_H_

#include <vector>
#include <memory>
#include <map>
#include <unordered_map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/id.h"
#include "map-api/revision.h"
#include "core.pb.h"

namespace map_api {

class CRTable {
 public:
  /**
   * Default fields
   */
  static const std::string kIdField;
  static const std::string kInsertTimeField;
  /**
   * Init routine, may be overriden by derived classes, in particular
   * CRUTableInterface. This function calls the pure virtual functions
   * tableName() and define()
   */
  virtual bool init();

  bool isInitialized() const;

  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   * N.b. the singleton pattern protected functions should also be implemented,
   * see below
   * The singleton's static instance() also needs to be implemented, can't be
   * done here for static functions can't be virtual. Recommended to use
   * meyersInstance() to save typing.
   * Use protected destructor.
   */
  /**
   * This table name will appear in the database, so it must be chosen SQL
   * friendly: Letters and underscores only.
   */
  virtual const std::string name() const = 0;
  /**
   * Function to be implemented by derivations: Define table by repeated
   * calls to addField()
   */
  virtual void define() = 0;

  /**
   * Returns an empty revision having the structure as defined by the user
   * in define() TODO(tcies) cache, in setup()
   */
  std::shared_ptr<Revision> getTemplate() const;
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

 protected:
  /**
   * =====================================
   * Singleton pattern protected functions
   * =====================================
   */
  CRTable() = default;
  CRTable(const CRTable&) = delete;
  CRTable& operator=(const CRTable&) = delete;
  virtual ~CRTable();
  /**
   * Allows derived classes to implement Meyer's singleton pattern without need
   * to retype the entire instance() function
   * TODO(tcies) this could be a common, free function (rather: a macro)
   */
  template<typename ClassType>
  static ClassType& meyersInstance();

  /**
   * Function to be called at definition:  Adds field to table. This only calls
   * the other addField function with the proper enum, see implementation
   * header.
   */
  template<typename Type>
  void addField(const std::string& name);
  void addField(const std::string& name,
                proto::TableFieldDescriptor_Type type);
  /**
   * Shared pointer to database session
   * TODO(tcies) move to private, remove from testtable, replace by purgedb
   */
  std::shared_ptr<Poco::Data::Session> session_;

  /**
   * The following functions are to be used by transactions only. They pose a
   * very crude access straight to the database, without synchronization
   * and conflict checking - that is assumed to be done by the transaction.
   * History is another example at it is managed by the transaction.
   */
  friend class LocalTransaction;
  friend class History;
  /**
   * Commits an insert query. ID has to be defined in the query. Non-virtual
   * interface design pattern.
   */
  bool rawInsert(Revision& query) const;
  virtual bool rawInsertImpl(Revision& query) const;
  /**
   * Fetches row by ID and returns it as revision. Non-virtual interface
   * design pattern. "Sees" only values with lower or equal insert time.
   */
  std::shared_ptr<Revision> rawGetById(const Id& id, const Time& time) const;
  virtual std::shared_ptr<Revision> rawGetByIdImpl(const Id& id,
                                                   const Time& time) const;
  /**
   * Loads items where key = value, returns their count.
   * If "key" is an empty string, no filter will be applied (equivalent to
   * rawDump())
   * The non-templated override that uses a revision container for the value is
   * there so that class Transaction may store conflict requests, which call
   * this function upon commit, without the need to specialize, which would be
   * impractical for users who want to add custom field types.
   * Although rawFind can't be virtual final as it is templated, it is a
   * non-virtual interface.
   */
  template<typename ValueType>
  int rawFind(const std::string& key, const ValueType& value, const Time& time,
              std::unordered_map<Id, std::shared_ptr<Revision> >* dest) const;
  int rawFindByRevision(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest)  const;
  virtual int rawFindByRevisionImpl(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest)  const;
  /**
   * Same as rawFind(), but asserts that not more than one item is found.
   * As rawFind() and rawFindByRevision(), this is not meant to be overridden.
   */
  template<typename ValueType>
  std::shared_ptr<Revision> rawFindUnique(const std::string& key,
                                          const ValueType& value,
                                          const Time& time) const;
  /**
   * Fetches all the contents of the table. Calls rawFindByRevision indirectly.
   */
  void rawDump(const Time& time,
               std::unordered_map<Id, std::shared_ptr<Revision> >* dest) const;
  /**
   * The PocoToProto class serves as intermediate between Poco and Protobuf:
   * Because Protobuf doesn't support pointers to numeric fields and Poco Data
   * can't handle blobs saved as std::strings (which is used in Protobuf),
   * this intermediate data structure is required to pass data from Poco::Data
   * to our protobuf objects.
   */
  class PocoToProto {
   public:
    /**
     * Associating with Table interface object to get template
     */
    PocoToProto(const CRTable& table);
    /**
     * To be inserted between "SELECT" and "FROM": Bind database outputs to
     * own structure.
     */
    void into(Poco::Data::Statement& statement);
    /**
     * Applies the data obtained after statement execution onto a vector of
     * Protos. Returns the element count.
     */
    int toProto(std::vector<std::shared_ptr<Revision> >* dest);
   private:
    const CRTable& table_;
    /**
     * Maps where the data is store intermediately
     */
    std::map<std::string, std::vector<double> > doubles_;
    std::map<std::string, std::vector<int32_t> > ints_;
    std::map<std::string, std::vector<int64_t> > longs_;
    std::map<std::string, std::vector<Poco::Data::BLOB> > blobs_;
    std::map<std::string, std::vector<std::string> > strings_;
    std::map<std::string, std::vector<std::string> > hashes_;
  };

 private:
  /**
   * Handle with care - this in not thread-safe and is only intended to be used
   * for testing. It also doesn't synchronize with the metatable, so the latter
   * MUST BE KILLED BY THE USER.
   */
  void kill();
  /**
   * Synchronize with cluster: Check if table already present in cluster
   * metatable, add user to distributed table. Virtual so that the metatable
   * may override this to do nothing in order to avoid infinite recursion.
   */
  virtual bool sync();
  /**
   * Parse and execute SQL query necessary to create the table schema in the
   * database.
   */
  virtual bool createQuery();

  proto::TableDescriptor structure_;
  bool initialized_ = false;
};

std::ostream& operator<< (std::ostream& stream, const
                          CRTable::ItemDebugInfo& info);

} /* namespace map_api */

#include "map-api/cr-table-inl.h"

#endif /* MAP_API_CR_TABLE_H_ */
