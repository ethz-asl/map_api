#ifndef MAP_API_SQLITE_INTERFACE_H_
#define MAP_API_SQLITE_INTERFACE_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "map-api/cr-table.h"
#include "map-api/revision.h"
#include "map-api/table-descriptor.h"

namespace map_api {

/**
 * This class allows CR and CRU table implementations to share SQLite commands
 * without the need of one deriving of the other, thus avoiding the diamond
 * problem of multiple inheritance.
 * In general, no checks are made here unless explicitly specified.
 */
class SqliteInterface {
 public:
  virtual ~SqliteInterface();
  void init(const std::string& table_name,
            std::weak_ptr<Poco::Data::Session> session);
  /**
   * Checks whether table name and field names are fit for SQL.
   */
  bool isSqlSafe(const TableDescriptor& descriptor) const;
  /**
   * Creates (if not exists) the table as specified
   */
  bool create(const TableDescriptor& descriptor);
  /**
   * Inserts data as is from the supplied revision
   */
  bool insert(const Revision& to_insert);
  bool bulkInsert(const CRTable::InsertRevisionMap& to_insert);

  /**
   * Allows a to also feed custom commands to the database -
   * the intention of this class is not to hide away SQLite operations but
   * merely to avoid duplicate code in CR and CRU table implementations.
   */
  std::weak_ptr<Poco::Data::Session> getSession();
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
    explicit PocoToProto(const std::shared_ptr<Revision>& reference);
    /**
     * To be inserted between "SELECT" and "FROM": Bind database outputs to
     * own structure.
     */
    void into(Poco::Data::Statement* statement);
    /**
     * Applies the data obtained after statement execution onto a vector of
     * Protos. Returns the element count.
     */
    int toProto(std::vector<std::shared_ptr<Revision> >* dest) const;

   private:
    int resultSize() const;
    std::shared_ptr<Revision> reference_;
    /**
     * Maps where the data is store intermediately
     */
    std::map<int, std::vector<double> > doubles_;
    std::map<int, std::vector<Poco::Int32> > ints_;
    std::map<int, std::vector<Poco::Int64> > longs_;
    std::map<int, std::vector<Poco::UInt64> > ulongs_;
    std::map<int, std::vector<Poco::Data::BLOB> > blobs_;
    std::map<int, std::vector<std::string> > strings_;
    std::map<int, std::vector<std::string> > hashes_;
  };

 private:
  bool isSqlSafe(const std::string& string) const;
  std::weak_ptr<Poco::Data::Session> session_;
  std::string table_name_;
};

}  // namespace map_api

#endif  // MAP_API_SQLITE_INTERFACE_H_
