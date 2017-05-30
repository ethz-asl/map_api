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

#ifndef MAP_API_CORE_H_
#define MAP_API_CORE_H_

#include <mutex>

namespace map_api {
class Hub;
class NetTableManager;

/**
 * The Map API core class is the first interface between robot application and
 * the Map API system. It is a singleton in order to:
 * - Ensure that only one instance of the database is created and used
 * - Ensure that only one thread is present to communicate with other nodes
 */
class Core final {
 public:
  // Returns null iff core is not initialized yet. Waits on initialized_mutex_.
  static Core* instance();
  // Returns null if core is not initialized, or if initialized_mutex_ is
  // locked.
  static Core* instanceNoWait();

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
  // The following can malfunction if the only other peer leaves in the middle
  // of the execution of this function.
  void killOnceSharedUnlessAlone();

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
