#ifndef MAP_API_DISCOVERY_H_
#define MAP_API_DISCOVERY_H_

#include <string>
#include <vector>

#include "map-api/peer-id.h"

namespace map_api {

/**
 * For now, this class regulates discovery through /tmp/mapapi-discovery.txt .
 * In the future, this will be replaced by avahi for local networks or
 * satoshi-client style internet discovery.
 * This class guarantees inter-process locking for concurrent access to the
 * discovery file.
 */
class Discovery {
 public:
  /**
   * Announces own address to discovery.
   */
  void announce() const;
  /**
   * Populates "peers" with PeerIds from the discovery source. The peers are
   * not necessarily all reachable (that couldn't be guaranteed anyways).
   * The own address is ignored if present in the discovery source.
   * Returns the amount of found peers.
   */
  int getPeers(std::vector<PeerId>* peers) const;
  /**
   * Removes own address from discovery
   */
  void leave() const;
 private:
  void append(const std::string& new_content) const;
  void getFileContents(std::string* result) const;
  void lock() const;
  void replace(const std::string& new_content) const;
  void unlock() const;

  static const std::string kFileName;
  static const char kLockFileName[];

  /**
   * May only be used by the Hub
   */
  Discovery() = default;
  Discovery(const Discovery&) = delete;
  Discovery& operator=(const Discovery&) = delete;
  ~Discovery() = default;
  friend class MapApiHub;
};

} /* namespace map_api */

#endif /* MAP_API_DISCOVERY_H_ */
