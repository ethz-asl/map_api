#include <gflags/gflags.h>

DEFINE_uint64(subprocess_id, 0, "Identification of subprocess in case of "
              "multiprocess testing. 0 if master process.");
