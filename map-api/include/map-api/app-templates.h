// A header for all the templates that can or must be specialized by Map API
// applications.

#ifndef MAP_API_APP_TEMPLATES_H_
#define MAP_API_APP_TEMPLATES_H_

#include <memory>

#include <multiagent-mapping-common/unique-id.h>

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

template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
TrackerIdType determineTracker(const TrackeeType& trackee);

#define MAP_API_SIMPLE_TYPE_REVISION_CONVERSION(Type, ProtoType)             \
  template <>                                                                \
  std::shared_ptr<Type> objectFromRevision<Type>(const Revision& revision) { \
    std::shared_ptr<Type> result(new Type);                                  \
    ProtoType proto;                                                         \
    revision.get(0, &proto);                                                 \
    result->deserialize(proto);                                              \
    return result;                                                           \
  }                                                                          \
  template <>                                                                \
  void objectToRevision(const Type& object, Revision* revision) {            \
    CHECK_NOTNULL(revision);                                                 \
    ProtoType proto;                                                         \
    object.serialize(&proto);                                                \
    revision->set(0, proto);                                                 \
  }

#define MAP_API_SIMPLE_TYPE_WITH_ID_REVISION_CONVERSION(Type, ProtoType,     \
                                                        IdType)              \
  template <>                                                                \
  std::shared_ptr<Type> objectFromRevision<Type>(const Revision& revision) { \
    std::shared_ptr<Type> result(new Type);                                  \
    ProtoType proto;                                                         \
    revision.get(0, &proto);                                                 \
    result->deserialize(revision.getId<IdType>(), proto);                    \
    return result;                                                           \
  }                                                                          \
  template <>                                                                \
  void objectToRevision(const Type& object, Revision* revision) {            \
    CHECK_NOTNULL(revision);                                                 \
    ProtoType proto;                                                         \
    object.serialize(&proto);                                                \
    revision->set(0, proto);                                                 \
    revision->setId(object.id());                                            \
  }

}  // namespace map_api

#endif  // MAP_API_APP_TEMPLATES_H_
