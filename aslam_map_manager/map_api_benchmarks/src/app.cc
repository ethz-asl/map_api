#include "map_api_benchmarks/app.h"

#include <memory>

#include <map-api/map-api-core.h>
#include <map-api/net-table-manager.h>
#include <map-api/revision.h>
#include <map-api/table-descriptor.h>
#include <multiagent_mapping_common/eigen-matrix-proto.h>

#include <map_api_benchmarks/association.h>
#include <map_api_benchmarks/center.h>
#include <map_api_benchmarks/data-point.h>

namespace map_api {
REVISION_PROTOBUF(common::EigenMatrixProto);
REVISION_ENUM(map_api::benchmarks::Association, proto::TableFieldDescriptor_Type_BLOB);
REVISION_ENUM(map_api::benchmarks::Center, proto::TableFieldDescriptor_Type_BLOB);
REVISION_ENUM(map_api::benchmarks::DataPoint, proto::TableFieldDescriptor_Type_BLOB);
REVISION_SET(map_api::benchmarks::Association) {
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
REVISION_GET(map_api::benchmarks::Association) {
  bool parsed = value->parse(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse map_api::benchmarks::Association";
    return false;
  }
  return true;
}
REVISION_SET(map_api::benchmarks::Center) {
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
REVISION_GET(map_api::benchmarks::Center) {
  bool parsed = value->parse(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse map_api::benchmarks::Center";
    return false;
  }
  return true;
}
REVISION_SET(map_api::benchmarks::DataPoint) {
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
REVISION_GET(map_api::benchmarks::DataPoint) {
  bool parsed = value->parse(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse map_api::benchmarks::DataPoint";
    return false;
  }
  return true;
}
} // namespace map_api

namespace map_api {
namespace benchmarks {
namespace app {
const std::string kAssociationTableName =
    "map_api_benchmarks_association_table";
const std::string kCenterTableCenterIdField = "center_id";

const std::string kDataPointTableName = "map_api_benchmarks_data_point_table";
const std::string kDataPointTable_Data_Field = "data";

const std::string kCenterTableName = "map_api_benchmarks_center_table";
const std::string kCenterTable_Data_Field = "data";

map_api::NetTable* data_point_table = nullptr;
map_api::NetTable* center_table = nullptr;
map_api::NetTable* association_table = nullptr;

void init() {
  map_api::MapApiCore::initializeInstance();
  std::unique_ptr<map_api::TableDescriptor> descriptor;

  // Association table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kAssociationTableName);
  descriptor->addField<map_api::Id>(kDataPointTableDataPointIdField);
  descriptor->addField<map_api::Id>(kCenterTableCenterIdField);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CRU, &descriptor);
  association_table = &map_api::NetTableManager::instance().
      getTable(kAssociationTableName);

  // Data point table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kDataPointTableName);
  descriptor->addField<common::EigenMatrixProto>(kDataPointTable_Data_Field);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CR, &descriptor);
  data_point_table = &map_api::NetTableManager::instance().
      getTable(kDataPointTableName);

  // Center table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kCenterTableName);
  descriptor->addField<common::EigenMatrixProto>(kCenterTable_Data_Field);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CRU, &descriptor);
  center_table = &map_api::NetTableManager::instance().
      getTable(kCenterTableName);
}

void kill() {
  map_api::MapApiCore::instance()->kill();
}

void associationFromRevision(const map_api::Revision& revision,
                             map_api::benchmarks::Association* association) {
  CHECK_NOTNULL(association);
  sm::HashId datapoint_hash_id;
  sm::HashId center_hash_id;
  revision.get(map_api::CRTable::kIdField, &datapoint_hash_id);
  revision.get(kCenterTableCenterIdField, &center_hash_id);
}

void dataPointFromRevision(const map_api::Revision& revision,
                           map_api::benchmarks::DataPoint* data_point) {
  CHECK_NOTNULL(data_point);

  sm::HashId data_point_hash_id;
  revision.get(map_api::CRTable::kIdField, &data_point_hash_id);
  common::EigenMatrixProto data_point_data;
  revision.get(app::kDataPointTable_Data_Field, &data_point_data);

  Eigen::VectorXd data_point_data_eigen;
  data_point_data.deserialize(&data_point_data_eigen);

  data_point->setData(data_point_data_eigen);
  map_api::benchmarks::DataPointId data_point_id;
  data_point_id.fromHashId(data_point_hash_id);
  data_point->setId(data_point_id);
}

void centerFromRevision(const map_api::Revision& revision,
                        map_api::benchmarks::Center* center) {
  CHECK_NOTNULL(center);

  sm::HashId center_hash_id;
  revision.get(map_api::CRTable::kIdField, &center_hash_id);
  common::EigenMatrixProto center_data;
  revision.get(app::kCenterTable_Data_Field, &center_data);

  Eigen::VectorXd center_data_eigen;
  center_data.deserialize(&center_data_eigen);

  center->setData(center_data_eigen);
  map_api::benchmarks::CenterId center_id;
  center_id.fromHashId(center_hash_id);
  center->setId(center_id);
}

void associationToRevision(const map_api::benchmarks::Association& association,
                           map_api::Revision* revision) {
  CHECK_NOTNULL(revision);

  sm::HashId id;
  association.id().toHashId(&id);
  revision->set(map_api::CRTable::kIdField, id);
  sm::HashId center_id;
  association.centerId().toHashId(&center_id);
  revision->set(app::kCenterTableCenterIdField, center_id);
}

void centerToRevision(const map_api::benchmarks::Center& center,
                      map_api::Revision* revision) {
  CHECK_NOTNULL(revision);

  sm::HashId id;
  center.id().toHashId(&id);

  common::EigenMatrixProto data_proto;
  data_proto.serialize(center.getData());
  revision->set(map_api::CRTable::kIdField, id);
  revision->set(app::kCenterTable_Data_Field, data_proto);
}

void dataPointToRevision(const map_api::benchmarks::DataPoint& data_point,
                         map_api::Revision* revision) {
  CHECK_NOTNULL(revision);

  sm::HashId id;
  data_point.id().toHashId(&id);

  common::EigenMatrixProto data_proto;
  data_proto.serialize(data_point.getData());
  revision->set(map_api::CRTable::kIdField, id);
  revision->set(app::kDataPointTable_Data_Field, data_proto);
}

}  // namespace app
}  // namespace benchmarks
}  // namespace map_api
