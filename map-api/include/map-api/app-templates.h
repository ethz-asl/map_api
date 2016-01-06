// A header for all the templates that can or must be specialized by Map API
// applications.

#ifndef MAP_API_APP_TEMPLATES_H_
#define MAP_API_APP_TEMPLATES_H_

#include <memory>
#include <string>

#include <glog/logging.h>
#include <multiagent-mapping-common/unique-id.h>

#include "map-api/revision.h"

namespace map_api {
class NetTable;

template <typename ObjectType>
NetTable* tableForType();
#define MAP_API_TABLE_FOR_TYPE(Type, TableCPtr) \
  template <>                                   \
  NetTable* tableForType<Type>() {              \
    return TableCPtr;                           \
  }

template <typename ObjectType>
void objectFromRevision(const map_api::Revision& revision, ObjectType* result);
template <>
void objectFromRevision<std::string>(const map_api::Revision& revision,
                                     std::string* result);

template <typename ObjectType>
void objectToRevision(const ObjectType& object, map_api::Revision* revision);
template <>
void objectToRevision(const std::string& object, map_api::Revision* revision);

template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
TrackerIdType determineTracker(const TrackeeType& trackee);

template <typename ObjectType>
std::string getComparisonString(const ObjectType& a, const ObjectType& b);

template <typename ObjectType>
std::string getComparisonString(const std::shared_ptr<ObjectType>& a,
                                const std::shared_ptr<ObjectType>& b) {
  CHECK(a);
  CHECK(b);
  return a->getComparisonString(*b);
}

template <typename ObjectType>
std::string getComparisonString(const ObjectType& a, const ObjectType& b) {
  return a.getComparisonString(b);
}

#define MAP_API_SIMPLE_TYPE_REVISION_CONVERSION(Type, ProtoType)    \
  template <>                                                       \
  void objectFromRevision(const Revision& revision, Type* result) { \
    CHECK_NOTNULL(result);                                          \
    ProtoType proto;                                                \
    CHECK_EQ(revision.customFieldCount(), 1);                       \
    constexpr int kUniqueFieldIndex = 0;                            \
    revision.get(kUniqueFieldIndex, &proto);                        \
    result->deserialize(proto);                                     \
  }                                                                 \
  template <>                                                       \
  void objectToRevision(const Type& object, Revision* revision) {   \
    CHECK_NOTNULL(revision);                                        \
    ProtoType proto;                                                \
    object.serialize(&proto);                                       \
    CHECK_EQ(revision->customFieldCount(), 1);                      \
    constexpr int kUniqueFieldIndex = 0;                            \
    revision->set(kUniqueFieldIndex, proto);                        \
  }

#define MAP_API_SIMPLE_SHARED_REVISION_CONVERSION(Type, ProtoType) \
  template <>                                                      \
  void objectFromRevision(const Revision& revision,                \
                          std::shared_ptr<Type>* result) {         \
    CHECK_NOTNULL(result)->reset(new Type);                        \
    ProtoType proto;                                               \
    CHECK_EQ(revision.customFieldCount(), 1);                      \
    constexpr int kUniqueFieldIndex = 0;                           \
    revision.get(kUniqueFieldIndex, &proto);                       \
    (*result)->deserialize(proto);                                 \
  }                                                                \
  template <>                                                      \
  void objectToRevision(const std::shared_ptr<Type>& object,       \
                        Revision* revision) {                      \
    CHECK(object);                                                 \
    CHECK_NOTNULL(revision);                                       \
    ProtoType proto;                                               \
    object->serialize(&proto);                                     \
    CHECK_EQ(revision->customFieldCount(), 1);                     \
    constexpr int kUniqueFieldIndex = 0;                           \
    revision->set(kUniqueFieldIndex, proto);                       \
  }

#define MAP_API_SIMPLE_SHARED_WITH_ID_REVISION_CONVERSION(Type, ProtoType, \
                                                          IdType)          \
  template <>                                                              \
  void objectFromRevision(const Revision& revision,                        \
                          std::shared_ptr<Type>* result) {                 \
    CHECK_NOTNULL(result)->reset(new Type);                                \
    ProtoType proto;                                                       \
    CHECK_EQ(revision.customFieldCount(), 1);                              \
    constexpr int kUniqueFieldIndex = 0;                                   \
    revision.get(kUniqueFieldIndex, &proto);                               \
    (*result)->deserialize(revision.getId<IdType>(), proto);               \
  }                                                                        \
  template <>                                                              \
  void objectToRevision(const std::shared_ptr<Type>& object,               \
                        Revision* revision) {                              \
    CHECK(object);                                                         \
    CHECK_NOTNULL(revision);                                               \
    ProtoType proto;                                                       \
    object->serialize(&proto);                                             \
    CHECK_EQ(revision->customFieldCount(), 1);                             \
    constexpr int kUniqueFieldIndex = 0;                                   \
    revision->set(kUniqueFieldIndex, proto);                               \
    revision->setId(object->id());                                         \
  }

#define MAP_API_SIMPLE_SHARED_DERIVED_WITH_ID_REVISION_CONVERSION( \
    BaseType, DerivedType, ProtoType, IdType)                      \
  template <>                                                      \
  void objectFromRevision(const Revision& revision,                \
                          std::shared_ptr<BaseType>* result) {     \
    CHECK_NOTNULL(result);                                         \
    std::shared_ptr<DerivedType> derived(new DerivedType);         \
    ProtoType proto;                                               \
    CHECK_EQ(revision.customFieldCount(), 1);                      \
    constexpr int kUniqueFieldIndex = 0;                           \
    revision.get(kUniqueFieldIndex, &proto);                       \
    derived->deserialize(revision.getId<IdType>(), proto);         \
    *result = derived;                                             \
  }                                                                \
  template <>                                                      \
  void objectToRevision(const std::shared_ptr<BaseType>& object,   \
                        Revision* revision) {                      \
    CHECK(object);                                                 \
    CHECK_NOTNULL(revision);                                       \
    std::shared_ptr<DerivedType> derived =                         \
        std::static_pointer_cast<DerivedType>(object);             \
    ProtoType proto;                                               \
    derived->serialize(&proto);                                    \
    CHECK_EQ(revision->customFieldCount(), 1);                     \
    constexpr int kUniqueFieldIndex = 0;                           \
    revision->set(kUniqueFieldIndex, proto);                       \
    revision->setId(derived->id());                                \
  }

}  // namespace map_api

#endif  // MAP_API_APP_TEMPLATES_H_
