#include "map-api/internal/trackee-multimap.h"

#include "map-api/net-table-manager.h"
#include "./core.pb.h"

namespace map_api {

void TrackeeMultimap::deserialize(const proto::Revision& proto) {
  for (int i = 0; i < proto.chunk_tracking_size(); ++i) {
    const proto::TableChunkTracking& table_trackees = proto.chunk_tracking(i);
    NetTable* table =
        &NetTableManager::instance().getTable(table_trackees.table_name());
    for (int j = 0; j < table_trackees.chunk_ids_size(); ++j) {
      Id chunk_id;
      chunk_id.deserialize(table_trackees.chunk_ids(j));
      emplace(table, chunk_id);
    }
  }
}

void TrackeeMultimap::serialize(proto::Revision* proto) const {
  proto->mutable_chunk_tracking()->Clear();
  proto::TableChunkTracking* proto_table_trackee = nullptr;
  for (const value_type& trackee : *this) {
    if (proto_table_trackee == nullptr ||
        proto_table_trackee->table_name() != trackee.first->name()) {
      proto_table_trackee = proto->add_chunk_tracking();
      proto_table_trackee->set_table_name(trackee.first->name());
    }
    trackee.second.serialize(proto_table_trackee->add_chunk_ids());
  }
}

}  // namespace map_api
