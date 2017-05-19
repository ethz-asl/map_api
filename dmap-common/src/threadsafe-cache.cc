#include "dmap-common/threadsafe-cache.h"

#include <gflags/gflags.h>

DEFINE_bool(cache_blame_dirty, false,
            "Report unique backtraces where "
            "threadsafe cache has been dirtied from.");

DEFINE_bool(cache_blame_insert, false,
            "Report unique backtraces where "
            "threadsafe cache has been inserted to from.");

DEFINE_uint64(cache_blame_dirty_sampling, 1u,
              "Will backtrace dirty accesses once every n times.");
