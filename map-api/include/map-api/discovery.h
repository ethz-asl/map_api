// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef DMAP_DISCOVERY_H_
#define DMAP_DISCOVERY_H_

#include <vector>

#include "map-api/peer-id.h"

namespace map_api {

/**
 * Class for discovery of other peers. Use lock() and unlock() for
 * synchronization.
 */
class Discovery {
 public:
  virtual ~Discovery() {}  // unique pointer needs destructor
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
  inline void leave() { remove(PeerId::self()); }
  virtual void lock() = 0;
  virtual void remove(const PeerId& peer) = 0;
  virtual void unlock() = 0;
};

} // namespace map_api

#endif  // DMAP_DISCOVERY_H_
