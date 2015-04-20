#include "map-api/trackee-multimap.h"

#include <iterator>

#include <glog/logging.h>

#include "map-api/net-table-manager.h"
#include "./core.pb.h"

namespace map_api {

void TrackeeMultimap::deserialize(const proto::Revision& proto) {
  for (int i = 0; i < proto.chunk_tracking_size(); ++i) {
    const proto::TableChunkTracking& table_trackees = proto.chunk_tracking(i);
    NetTable* table =
        &NetTableManager::instance().getTable(table_trackees.table_name());
    for (int j = 0; j < table_trackees.chunk_ids_size(); ++j) {
      common::Id chunk_id;
      chunk_id.deserialize(table_trackees.chunk_ids(j));
      operator[](table).emplace(chunk_id);
    }
  }
}

void TrackeeMultimap::deserialize(const Revision& revision) {
  deserialize(*revision.underlying_revision_);
}

void TrackeeMultimap::serialize(proto::Revision* proto) const {
  proto->mutable_chunk_tracking()->Clear();
  for (const value_type& table_trackees : *this) {
    proto::TableChunkTracking* proto_table_trackee =
        proto->add_chunk_tracking();
    proto_table_trackee->set_table_name(table_trackees.first->name());
    for (const common::Id& trackee : table_trackees.second) {
      trackee.serialize(proto_table_trackee->add_chunk_ids());
    }
  }
}

void TrackeeMultimap::serialize(Revision* revision) const {
  serialize(revision->underlying_revision_.get());
}

void TrackeeMultimap::merge(const TrackeeMultimap& other) {
  for (const value_type& table_trackees : other) {
    iterator found = find(table_trackees.first);
    if (found == end()) {
      emplace(table_trackees);
    } else {
      for (const common::Id& trackee : table_trackees.second) {
        found->second.emplace(trackee);
      }
    }
  }
}

bool TrackeeMultimap::hasOverlap(const TrackeeMultimap& other) const {
  for (const value_type& table_trackees : *this) {
    const_iterator found = other.find(table_trackees.first);
    if (found == other.end()) {
      continue;
    }
    for (const common::Id& trackee : table_trackees.second) {
      if (found->second.find(trackee) == found->second.end()) {
        return true;
      }
    }
  }
  return false;
}

bool TrackeeMultimap::isSameVerbose(const TrackeeMultimap& other) const {
  if (size() != other.size()) {
    LOG(WARNING) << "Table counts mismatch!";
    return false;
  }

  for (const value_type& table_trackees : *this) {
    const_iterator found = other.find(table_trackees.first);
    if (found == other.end()) {
      LOG(WARNING) << "Table " << table_trackees.first->name()
                   << " not represented in other!";
      return false;
    }

    if (table_trackees.second != found->second) {
      LOG(WARNING) << "Trackees for table " << table_trackees.first->name()
                   << " mismatch!";
      return false;
    }
  }
  return true;
}

}  // namespace map_api
