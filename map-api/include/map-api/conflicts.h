#ifndef MAP_API_CONFLICTS_H_
#define MAP_API_CONFLICTS_H_

#include <list>
#include <memory>
#include <sstream>  // NOLINT
#include <string>
#include <unordered_map>

#include "map-api/net-table.h"

namespace map_api {
class Revision;

struct Conflict {
  const std::shared_ptr<const Revision> theirs;
  const std::shared_ptr<const Revision> ours;
};

// Choosing list for constant splicing, linear iteration is fine.
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

  // Requires ObjectType to have function
  // std::string getComparisonString(const ObjectType&) const;
  template <typename ObjectType>
  std::string debugConflictsInTable(NetTable* table) {
    CHECK(table);
    iterator found = find(table);
    if (found == end()) {
      return "There are no conflicts in table " + table->name() + "!\n";
    }
    std::string result;
    for (const Conflict& conflict : found->second) {
      ObjectType our_object, their_object;
      objectFromRevision(*conflict.ours, &our_object);
      objectFromRevision(*conflict.theirs, &their_object);
      result += "For object " +
                conflict.ours->getId<common::Id>().printString() +
                " of table " + table->name() +
                " the attempted commit compares to the conflict as follows:\n" +
                getComparisonString(our_object, their_object) + "\n";
    }
    return result;
  }
};

}  // namespace map_api

#endif  // MAP_API_CONFLICTS_H_
