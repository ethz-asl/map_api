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
  static const char kFileName[];

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
  FileDiscovery();
  FileDiscovery(const FileDiscovery&) = delete;
  FileDiscovery& operator=(const FileDiscovery&) = delete;
  friend class Hub;

  bool force_unlocked_once_;
};

}  // namespace map_api

#endif  // MAP_API_FILE_DISCOVERY_H_
