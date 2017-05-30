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

#ifndef DMAP_LEGACY_CHUNK_DATA_RAM_CONTAINER_H_
#define DMAP_LEGACY_CHUNK_DATA_RAM_CONTAINER_H_

#include <vector>

#include "map-api/legacy-chunk-data-container-base.h"

namespace map_api {

class LegacyChunkDataRamContainer : public LegacyChunkDataContainerBase {
 public:
  virtual ~LegacyChunkDataRamContainer();

 private:
  virtual bool initImpl() final override;
  virtual bool insertImpl(const std::shared_ptr<const Revision>& query)
      final override;
  virtual bool bulkInsertImpl(const MutableRevisionMap& query) final override;
  virtual bool patchImpl(const std::shared_ptr<const Revision>& query)
      final override;
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const map_api_common::Id& id, const LogicalTime& time) const final override;
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const final override;
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const final override;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<map_api_common::Id>* ids) const
      final override;

  virtual bool insertUpdatedImpl(const std::shared_ptr<Revision>& query)
      final override;
  virtual void findHistoryByRevisionImpl(int key, const Revision& valueHolder,
                                         const LogicalTime& time,
                                         HistoryMap* dest) const final override;
  virtual void chunkHistory(const map_api_common::Id& chunk_id, const LogicalTime& time,
                            HistoryMap* dest) const final override;
  virtual void itemHistoryImpl(const map_api_common::Id& id, const LogicalTime& time,
                               History* dest) const final override;
  virtual void clearImpl() final override;

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder, const LogicalTime& time,
      const std::function<void(const map_api_common::Id& id,
                               const std::shared_ptr<const Revision>& item)>&
          action) const;
  inline void forChunkItemsAtTime(
      const map_api_common::Id& chunk_id, const LogicalTime& time,
      const std::function<void(const map_api_common::Id& id,
                               const std::shared_ptr<const Revision>& item)>&
          action) const;
  inline void trimToTime(const LogicalTime& time, HistoryMap* subject) const;

  HistoryMap data_;
};

}  // namespace map_api

#endif  // DMAP_LEGACY_CHUNK_DATA_RAM_CONTAINER_H_
