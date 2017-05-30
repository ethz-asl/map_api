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

#ifndef DMAP_PEER_ID_H_
#define DMAP_PEER_ID_H_

#include <iostream>  // NOLINT
#include <string>
#include <unordered_set>
#include <vector>

namespace map_api {

class PeerId {
 public:
  PeerId();

  explicit PeerId(const std::string& ip_port);

  /**
   * checks whether serialized PeerId string is valid
   */
  static bool isValid(const std::string& serialized);

  PeerId& operator=(const PeerId& other);

  static PeerId self();

  /**
   * Rank compared to other peers in network.
   */
  static size_t selfRank();

  const std::string& ipPort() const;

  bool operator<(const PeerId& other) const;

  bool operator==(const PeerId& other) const;

  bool operator!=(const PeerId& other) const;

  bool isValid() const;

 private:
  static const std::string kInvalidAdress;

  std::string ip_port_;
};

typedef std::vector<PeerId> PeerIdList;
typedef std::unordered_set<PeerId> PeerIdSet;

} // namespace map_api

namespace std {

inline ostream& operator<<(ostream& out, const map_api::PeerId& peer_id) {
  out << "IpPort(" << peer_id.ipPort() << ")";
  return out;
}

template <>
struct hash<map_api::PeerId> {
  std::size_t operator()(const map_api::PeerId& peer_id) const {
    return std::hash<std::string>()(peer_id.ipPort());
  }
};

} // namespace std

#endif  // DMAP_PEER_ID_H_
