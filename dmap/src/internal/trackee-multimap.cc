#include "dmap/trackee-multimap.h"

#include <iterator>

#include <glog/logging.h>

#include "dmap/net-table-manager.h"
#include "./core.pb.h"

namespace dmap {

void TrackeeMultimap::deserialize(const proto::Revision& proto) {
  for (int i = 0; i < proto.chunk_tracking_size(); ++i) {
    const proto::TableChunkTracking& table_trackees = proto.chunk_tracking(i);
    NetTable* table =
        &NetTableManager::instance().getTable(table_trackees.table_name());
    for (int j = 0; j < table_trackees.chunk_ids_size(); ++j) {
      dmap_common::Id chunk_id;
      chunk_id.deserialize(table_trackees.chunk_ids(j));
      operator[](table).emplace(chunk_id);
    }
  }
}

void TrackeeMultimap::deserialize(const Revision& revision) {
  CHECK_NOTNULL(revision.underlying_revision_.get());
  deserialize(*revision.underlying_revision_);
}

void TrackeeMultimap::serialize(proto::Revision* proto) const {
  proto->mutable_chunk_tracking()->Clear();
  for (const value_type& table_trackees : *this) {
    proto::TableChunkTracking* proto_table_trackee =
        proto->add_chunk_tracking();
    proto_table_trackee->set_table_name(table_trackees.first->name());
    for (const dmap_common::Id& trackee : table_trackees.second) {
      trackee.serialize(proto_table_trackee->add_chunk_ids());
    }
  }
}

void TrackeeMultimap::serialize(Revision* revision) const {
  CHECK_NOTNULL(revision->underlying_revision_.get());
  serialize(revision->underlying_revision_.get());
}

bool TrackeeMultimap::merge(const TrackeeMultimap& other) {
  bool has_change = false;
  for (const value_type& table_trackees : other) {
    iterator found = find(table_trackees.first);
    if (found == end()) {
      emplace(table_trackees);
      has_change = true;
    } else {
      for (const dmap_common::Id& trackee : table_trackees.second) {
        if (found->second.emplace(trackee).second) {
          has_change = true;
        }
      }
    }
  }
  return has_change;
}

bool TrackeeMultimap::hasOverlap(const TrackeeMultimap& other) const {
  for (const value_type& table_trackees : *this) {
    const_iterator found = other.find(table_trackees.first);
    if (found == other.end()) {
      continue;
    }
    for (const dmap_common::Id& trackee : table_trackees.second) {
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

}  // namespace dmap
