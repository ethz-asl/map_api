#ifndef MAP_API_BENCHMARKS_APP_H_
#define MAP_API_BENCHMARKS_APP_H_

#include <string>

#include <map-api/net-table.h>

#include "map_api_benchmarks/common.h"

namespace map_api {
namespace benchmarks {
namespace app {
void init();
void kill();

extern const std::string kDataPointTableName;
enum AssociationTableFields {
  kAssociationTableCenterIdField
};

extern const std::string kCenterTableName;
enum DataPointTableFields {
  kDataPointTableDataField
};

extern const std::string kAssociationTableName;
enum CenterTableFields {
  kCenterTableDataField
};

extern map_api::NetTable* association_table;
extern map_api::NetTable* data_point_table;
extern map_api::NetTable* center_table;

void descriptorFromRevision(const map_api::Revision& revision,
                            DescriptorType* descriptor);
void centerFromRevision(const map_api::Revision& revision,
                        DescriptorType* center);
void membershipFromRevision(const map_api::Revision& revision,
                            Id* descriptor_id, Id* center_id);

void descriptorToRevision(const DescriptorType& descriptor, const Id& id,
                          map_api::Revision* revision);
void centerToRevision(const DescriptorType& center, const Id& id,
                      map_api::Revision* revision);
void membershipToRevision(const Id& descriptor_id, const Id& center_id,
                          map_api::Revision* revision);

}  // namespace app
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_APP_H_
