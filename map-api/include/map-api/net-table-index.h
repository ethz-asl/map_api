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

#ifndef DMAP_NET_TABLE_INDEX_H_
#define DMAP_NET_TABLE_INDEX_H_

#include <string>
#include <unordered_set>

#include "map-api/chord-index.h"
#include "map-api/peer-handler.h"

namespace map_api_common {
class Id;
}  // namespace common

namespace map_api {
class NetTableIndex : public ChordIndex {
 public:
  virtual ~NetTableIndex();
  void handleRoutedRequest(const Message& routed_request, Message* response);

  /**
   * Without guarantee of consistency - the only thing that is (needed to be)
   * guaranteed is that if at least one peer holds a chunk, at least one peer
   * will be registered in the index.
   */
  void seekPeers(const map_api_common::Id& chunk_id, std::unordered_set<PeerId>* peers);
  void announcePosession(const map_api_common::Id& chunk_id);
  void renouncePosession(const map_api_common::Id& chunk_id);

  static const char kRoutedChordRequest[];
  static const char kPeerResponse[];
  static const char kGetClosestPrecedingFingerRequest[];
  static const char kGetSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kLockRequest[];
  static const char kUnlockRequest[];
  static const char kNotifyRequest[];
  static const char kReplaceRequest[];
  static const char kAddDataRequest[];
  static const char kRetrieveDataRequest[];
  static const char kRetrieveDataResponse[];
  static const char kFetchResponsibilitiesRequest[];
  static const char kFetchResponsibilitiesResponse[];
  static const char kPushResponsibilitiesRequest[];

 private:
  /**
   * Life cycle managed by NetTable!
   */
  explicit NetTableIndex(const std::string& table_name);
  NetTableIndex(const NetTableIndex&) = delete;
  NetTableIndex& operator=(const NetTableIndex&) = delete;
  friend class NetTable;

  bool rpc(const PeerId& to, const Message& request, Message* response);

  virtual bool getClosestPrecedingFingerRpc(const PeerId& to, const Key& key,
                                            PeerId* closest_preceding)
      final override;
  virtual bool getSuccessorRpc(const PeerId& to,
                               PeerId* predecessor) final override;
  virtual bool getPredecessorRpc(const PeerId& to,
                                 PeerId* predecessor) final override;
  virtual bool lockRpc(const PeerId& to) final override;
  virtual bool unlockRpc(const PeerId& to) final override;
  virtual bool notifyRpc(const PeerId& to,
                         const PeerId& subject) final override;
  virtual bool replaceRpc(const PeerId& to, const PeerId& old_peer,
                          const PeerId& new_peer) final override;
  virtual bool addDataRpc(const PeerId& to, const std::string& key,
                          const std::string& value) final override;
  virtual bool retrieveDataRpc(const PeerId& to, const std::string& key,
                               std::string* value) final override;
  virtual bool fetchResponsibilitiesRpc(
      const PeerId& to, DataMap* responsibilities) final override;
  virtual bool pushResponsibilitiesRpc(
      const PeerId& to, const DataMap& responsibilities) final override;

  std::string table_name_;
  PeerHandler peers_;
};

} // namespace map_api

#endif  // DMAP_NET_TABLE_INDEX_H_
