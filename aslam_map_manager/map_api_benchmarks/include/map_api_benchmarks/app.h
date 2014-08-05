#ifndef MAP_API_BENCHMARKS_APP_H_
#define MAP_API_BENCHMARKS_APP_H_

#include <string>

#include <map-api/net-table.h>
#include <map_api_benchmarks/association.h>
#include <map_api_benchmarks/center.h>
#include <map_api_benchmarks/data-point.h>

namespace map_api {
namespace benchmarks {
namespace app {
void init();
void kill();

extern const std::string kDataPointTableName;
extern const std::string kDataPointTable_Data_Field;

extern const std::string kCenterTableName;
extern const std::string kCenterTable_Data_Field;

extern const std::string kAssociationTableName;
extern const std::string kDataPointTableDataPointIdField;
extern const std::string kCenterTableCenterIdField;

extern map_api::NetTable* association_table;
extern map_api::NetTable* data_point_table;
extern map_api::NetTable* center_table;

void associationFromRevision(const map_api::Revision& revision,
                             map_api::benchmarks::Association* association);
void dataPointFromRevision(const map_api::Revision& revision,
                           map_api::benchmarks::DataPoint* data_point);
void centerFromRevision(const map_api::Revision& revision,
                        map_api::benchmarks::Center* center);

void associationToRevision(const map_api::benchmarks::Association& association,
                           map_api::Revision* revision);
void dataPointToRevision(const map_api::benchmarks::DataPoint& data_point,
                         map_api::Revision* revision);
void centerToRevision(const map_api::benchmarks::Center& center,
                        map_api::Revision* revision);

}  // namespace app
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_APP_H_
