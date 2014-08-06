#include "map_api_benchmarks/app.h"

#include <memory>

#include <map-api/map-api-core.h>
#include <map-api/net-table-manager.h>
#include <map-api/revision.h>
#include <map-api/table-descriptor.h>
#include <multiagent_mapping_common/eigen-matrix-proto.h>

namespace map_api {
REVISION_PROTOBUF(common::EigenMatrixProto);
} // namespace map_api

namespace map_api {
namespace benchmarks {
namespace app {
const std::string kAssociationTableName =
    "map_api_benchmarks_association_table";
const std::string kAssociationTableCenterIdField = "center_id";

const std::string kDataPointTableName = "map_api_benchmarks_data_point_table";
const std::string kDataPointTableDataField = "data";

const std::string kCenterTableName = "map_api_benchmarks_center_table";
const std::string kCenterTableDataField = "data";

map_api::NetTable* data_point_table = nullptr;
map_api::NetTable* center_table = nullptr;
map_api::NetTable* association_table = nullptr;

void init() {
  map_api::MapApiCore::initializeInstance();
  std::unique_ptr<map_api::TableDescriptor> descriptor;

  // Association table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kAssociationTableName);
  descriptor->addField<map_api::Id>(kAssociationTableCenterIdField);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CRU, &descriptor);
  association_table = &map_api::NetTableManager::instance().
      getTable(kAssociationTableName);

  // Data point table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kDataPointTableName);
  descriptor->addField<common::EigenMatrixProto>(kDataPointTableDataField);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CR, &descriptor);
  data_point_table = &map_api::NetTableManager::instance().
      getTable(kDataPointTableName);

  // Center table.
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName(kCenterTableName);
  descriptor->addField<common::EigenMatrixProto>(kCenterTableDataField);
  map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CRU, &descriptor);
  center_table = &map_api::NetTableManager::instance().
      getTable(kCenterTableName);
}

void kill() {
  map_api::MapApiCore::instance()->kill();
}

void descriptorFromRevision(const map_api::Revision& revision,
                            DescriptorType* descriptor) {
  CHECK_NOTNULL(descriptor);
  common::EigenMatrixProto emp;
  revision.get(kDataPointTableDataField, &emp);
  emp.deserialize(descriptor);
}
void centerFromRevision(const map_api::Revision& revision,
                        DescriptorType* center) {
  CHECK_NOTNULL(center);
  common::EigenMatrixProto emp;
  revision.get(kCenterTableDataField, &emp);
  emp.deserialize(center);
}
void membershipFromRevision(const map_api::Revision& revision,
                            Id* descriptor_id, Id* center_id) {
  CHECK_NOTNULL(descriptor_id);
  CHECK_NOTNULL(center_id);
  revision.get(CRTable::kIdField, descriptor_id);
  revision.get(kAssociationTableCenterIdField, center_id);
}

void descriptorToRevision(const DescriptorType& descriptor, const Id& id,
                          map_api::Revision* revision) {
  CHECK_NOTNULL(revision);
  revision->set(CRTable::kIdField, id);
  common::EigenMatrixProto emp;
  emp.serialize(descriptor);
  revision->set(kDataPointTableDataField, emp);
}
void centerToRevision(const DescriptorType& center, const Id& id,
                      map_api::Revision* revision) {
  CHECK_NOTNULL(revision);
  revision->set(CRTable::kIdField, id);
  common::EigenMatrixProto emp;
  emp.serialize(center);
  revision->set(kCenterTableDataField, emp);
}
void membershipToRevision(const Id& descriptor_id, const Id& center_id,
                          map_api::Revision* revision){
  CHECK_NOTNULL(revision);
  revision->set(CRTable::kIdField, descriptor_id);
  revision->set(kAssociationTableCenterIdField, center_id);
}

}  // namespace app
}  // namespace benchmarks
}  // namespace map_api
