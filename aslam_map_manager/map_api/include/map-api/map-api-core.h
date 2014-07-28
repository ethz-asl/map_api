#ifndef MAP_API_CORE_HPP
#define MAP_API_CORE_HPP

#include <memory>

#include <gtest/gtest.h>

#include <Poco/Data/Common.h>

#include "map-api/cr-table-ram-cache.h"
#include "map-api/id.h"
#include "map-api/map-api-hub.h"
#include "map-api/net-table-manager.h"
#include "core.pb.h"

namespace map_api {

/**
 * The map api core class is the first interface between robot application and
 * the map api system. It is a singleton in order to:
 * - Ensure that only one instance of the database is created and used
 * - Ensure that only one thread is present to communicate with other nodes
 */
class MapApiCore final {
 public:
  /**
   * Get singleton instance of Map Api Core
   * Returns a null pointer if not initialized.
   */
  static MapApiCore* instance();
  static void initializeInstance();
  /**
   * Initializer
   */
  void init();
  /**
   * Check if initialized
   */
  bool isInitialized() const;
  /**
   * Makes the server thread re-enter, disconnects from database and removes
   * own address from discovery file.
   */
  void kill();

  //NetTableManager& tableManager();
  //const NetTableManager& tableManager() const;

 private:
  static const std::string kMetatableNameField;
  static const std::string kMetatableDescriptorField;
  /**
   * Constructor: Creates database if not existing, launches a new thread
   * that takes care of handling requests from other nodes.
   */
  MapApiCore();
  ~MapApiCore();
  /**
   * Returns a weak pointer to the database session. Static, i.e. decoupled from
   * instance() in order to avoid infinite recursion: This is called in
   * MapApiCore::init()
   */
  static std::weak_ptr<Poco::Data::Session> getSession();
  friend class CRTableRAMCache;
  friend class CRUTableRAMCache;
  friend class LocalTransaction;

  /**
   * Session of local database
   */
  static std::shared_ptr<Poco::Data::Session> db_session_;
  static bool db_session_initialized_;
  /**
   * Hub instance
   */
  MapApiHub& hub_;
  NetTableManager& table_manager_;

  static MapApiCore instance_;
  bool initialized_ = false;
  std::mutex initialized_mutex_;
};

}

#endif  // MAP_API_CORE_HPP
