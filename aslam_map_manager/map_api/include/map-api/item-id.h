#ifndef MAP_API_ITEM_ID_H
#define MAP_API_ITEM_ID_H

#include <glog/logging.h>

#include "map-api/id.h"
#include "map-api/cr-table.h"

namespace map_api {

/**
 * Item identification table reference and ID. Intended to be used as key value,
 * so ordering and hashing operations are provided.
 */
struct ItemId {
  Id id;
  CRTable* table;
  ItemId(const Id& _id, CRTable* _table) : table(_table), id(_id) {
    CHECK_NOTNULL(table);
  }
  inline bool operator <(const ItemId& other) const {
    if (table->name() == other.table->name()) {
      return id < other.id;
    }
    return table->name() < other.table->name();
  }
};

} // map_api

namespace std{

inline ostream& operator<<(ostream& out, const map_api::ItemId& item_id) {
  out << "(Table: " << item_id.table->name() << ", ID: " << item_id.id << ")";
  return out;
}

template<>
struct hash<map_api::ItemId>{
  typedef map_api::ItemId argument_type;
  typedef std::size_t value_type;

  value_type operator()(const argument_type& item_id) const {
    return std::hash<std::string>()(item_id.table->name()) ^
        std::hash<sm::HashId>()(item_id.id);
  }
};
} // namespace std

#endif // MAP_API_ITEM_ID_H
