#ifndef MAP_API_CORE_HPP
#define MAP_API_CORE_HPP

#include <memory>

#include <Poco/Data/Common.h>

#include "map-api/table-interface.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"

namespace map_api {

/**
 * The map api core class is the first interface between robot application and
 * the map api system. It is a singleton in order to:
 * - Ensure that only one instance of the database is created and used
 * - Ensure that only one thread is present to communicate with other nodes
 */
class MapApiCore {
 public:
  /**
   * Get singleton instance of Map Api Core
   * TODO(tcies) just make all functions static...
   */
  static MapApiCore& getInstance();
  /**
   * Get the list of available tables
   */
  std::shared_ptr<proto::TableList> getTables();
  /**
   * Get interface to given table by table name
   */
  std::shared_ptr<TableInterface> getTable(const std::string &name);
  /**
   * Initializer
   */
  bool init(const std::string &ipPort);
  /**
   * Check if initialized
   */
  bool isInitialized() const;

 private:
  /**
   * Constructor: Creates database if not existing, launches a new thread
   * that takes care of handling requests from other nodes.
   */
  MapApiCore();
  /**
   * Returns a shared pointer to the database session
   */
  std::shared_ptr<Poco::Data::Session> getSession();
  friend class TableInterface;
  friend class Transaction;
  /**
   * Session of local database
   */
  std::shared_ptr<Poco::Data::Session> dbSess_;
  /**
   * Hub instance
   */
  MapApiHub &hub_;
  /**
   * initialized?
   */
  bool initialized_;
};

}

#endif  // MAP_API_CORE_HPP
