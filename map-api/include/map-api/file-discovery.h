#ifndef MAP_API_FILE_DISCOVERY_H_
#define MAP_API_FILE_DISCOVERY_H_

#include <mutex>
#include <string>
#include <vector>

#include "map-api/discovery.h"

namespace map_api {
class PeerId;

/**
 * Regulates discovery through /tmp/mapapi-discovery.txt .
 */
class FileDiscovery final : public Discovery {
  friend class FileDiscoveryTest;

 public:
  static const std::string kFileName;

  virtual ~FileDiscovery();
  virtual void announce() final override;
  virtual int getPeers(std::vector<PeerId>* peers) final override;
  virtual void lock() final override;
  virtual void remove(const PeerId& peer) final override;
  virtual void unlock() final override;

 private:
  void append(const std::string& new_content) const;
  void getFileContents(std::string* result) const;
  void replace(const std::string& new_content) const;

  static const char kLockFileName[];
  static std::mutex mutex_;

  int lock_file_descriptor_ = -1;
  /**
   * May only be used by the Hub
   */
  FileDiscovery() = default;
  FileDiscovery(const FileDiscovery&) = delete;
  FileDiscovery& operator=(const FileDiscovery&) = delete;
  friend class Hub;
};

}  // namespace map_api

#endif  // MAP_API_FILE_DISCOVERY_H_
