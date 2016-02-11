#include "dmap/cache-base.h"

#include <gflags/gflags.h>

DEFINE_bool(dmap_prefetch_cache, false,
            "Will prefetch the entire cache at construction.");
DEFINE_bool(dmap_insert_into_existing_chunk, false,
            "Will insert into an existing chunk instead of creating one.");

namespace dmap {

CacheBase::~CacheBase() {}

}  // namespace dmap
