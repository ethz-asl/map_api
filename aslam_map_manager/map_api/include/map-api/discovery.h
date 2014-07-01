#ifndef MAP_API_DISCOVERY_H_
#define MAP_API_DISCOVERY_H_

#include <string>
#include <vector>

#include "map-api/peer-id.h"

namespace map_api {

/**
 * Class for discovery of other peers. Use lock() and unlock() for
 * synchronization.
 */
class Discovery {
 public:
  virtual ~Discovery() {}; // unique pointer needs destructor
  /**
   * Announces own address to discovery.
   */
  virtual void announce() = 0;
  /**
   * Populates "peers" with PeerIds from the discovery source. The peers are
   * not necessarily all reachable.
   * The own address is ignored if present in the discovery source.
   * Returns the amount of found peers.
   */
  virtual int getPeers(std::vector<PeerId>* peers) = 0;
  /**
   * Removes own address from discovery
   */
  inline void leave() {
    remove(PeerId::self());
  }
  virtual void lock() = 0;
  virtual void remove(const PeerId& peer) = 0;
  virtual void unlock() = 0;
 };

} /* namespace map_api */

#endif /* MAP_API_DISCOVERY_H_ */
