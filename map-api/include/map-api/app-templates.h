// A header for all the templates that can or must be specialized by Map API
// applications.

#ifndef MAP_API_APP_TEMPLATES_H_
#define MAP_API_APP_TEMPLATES_H_

#include <memory>

#include <map-api/unique-id.h>

namespace map_api {
class NetTable;
class Revision;

template <typename ObjectType>
NetTable* tableForType();
#define MAP_API_TABLE_FOR_TYPE(Type, TableCPtr) \
  template <>                                   \
  NetTable* tableForType<Type>() {              \
    return TableCPtr;                           \
  }

template <typename ObjectType>
std::shared_ptr<ObjectType> objectFromRevision(
    const map_api::Revision& revision);

template <typename ObjectType>
void objectToRevision(const ObjectType& object, map_api::Revision* revision);

template <typename TrackeeType, typename TrackerType>
Id determineTracker(const TrackeeType& trackee);

}  // namespace map_api

#endif  // MAP_API_APP_TEMPLATES_H_
