#ifndef MAP_API_CONFLICTS_H_
#define MAP_API_CONFLICTS_H_

#include <list>
#include <memory>
#include <string>
#include <sstream>  // NOLINT
#include <unordered_map>

#include "map-api/net-table.h"

namespace map_api {
class Revision;

struct Conflict {
  const std::shared_ptr<const Revision> theirs;
  const std::shared_ptr<const Revision> ours;
};

// constant splicing, linear iteration
class Conflicts : public std::list<Conflict> {};

class ConflictMap : public std::unordered_map<NetTable*, Conflicts> {
 public:
  inline std::string debugString() const {
    std::ostringstream ss;
    for (const value_type& pair : *this) {
      ss << pair.first->name() << ": " << pair.second.size() << " conflicts"
         << std::endl;
    }
    return ss.str();
  }
};

}  // namespace map_api

#endif  // MAP_API_CONFLICTS_H_
