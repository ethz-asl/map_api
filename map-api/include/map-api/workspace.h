#ifndef MAP_API_WORKSPACE_H_
#define MAP_API_WORKSPACE_H_

#include <initializer_list>

#include "map-api/trackee-multimap.h"

namespace map_api {

class Workspace : public TrackeeMultimap {
 public:
  static Workspace fullWorkspace();
  static Workspace fullWorkspace(
      const std::initializer_list<NetTable*>& tables);
};

}  // namespace map_api

#endif  // MAP_API_WORKSPACE_H_
