#ifndef MAP_API_CORE_H_
#define MAP_API_CORE_H_

#include <mutex>

namespace map_api {
class Hub;
class NetTableManager;

/**
 * The map api core class is the first interface between robot application and
 * the map api system. It is a singleton in order to:
 * - Ensure that only one instance of the database is created and used
 * - Ensure that only one thread is present to communicate with other nodes
 */
class Core final {
 public:
  /**
   * Get singleton instance of Map Api Core
   * Returns a null pointer if not initialized.
   */
  static Core* instance();
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
  /**
   * Same as kill, but makes sure each chunk has at least one other peer. Use
   * this only if you are sure that your data will be picked up by other peers.
   */
  void killOnceShared();

  // NetTableManager& tableManager();
  // const NetTableManager& tableManager() const;

 private:
  Core();
  ~Core();

  /**
   * Hub instance
   */
  Hub& hub_;
  NetTableManager& table_manager_;

  static Core instance_;
  bool initialized_ = false;
  std::mutex initialized_mutex_;
};
}  // namespace map_api

#endif  // MAP_API_CORE_H_
