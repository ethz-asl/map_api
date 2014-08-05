#ifndef MAP_API_BENCHMARKS_COMMON_H_
#define MAP_API_BENCHMARKS_COMMON_H_

#include <glog/logging.h>
#include "multiagent_mapping_common/unique_id.h"

namespace map_api {
namespace benchmarks {
UNIQUE_ID_DEFINE_ID(AssociationId);
UNIQUE_ID_DEFINE_ID(DataPointId);
UNIQUE_ID_DEFINE_ID(CenterId);
}  // namespace map_api
}  // namespace benchmarks

UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::AssociationId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::DataPointId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::CenterId);

#endif /* MAP_API_BENCHMARKS_COMMON_H_ */
