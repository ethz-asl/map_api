#include <map-api/cache-base.h>

#include <gflags/gflags.h>

DEFINE_bool(map_api_prefetch_cache, false,
            "Will prefetch the entire cache at construction.");
DEFINE_bool(insert_into_existing_chunk, true,
            "Will insert into an existing chunk instead of creating one.");

namespace map_api {

CacheBase::~CacheBase() {}

}  // namespace map_api
