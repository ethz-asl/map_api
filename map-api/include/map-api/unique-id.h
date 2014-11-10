#ifndef MAP_API_UNIQUE_ID_H_
#define MAP_API_UNIQUE_ID_H_
#include <map-api/internal/unique-id.h>

#include <atomic>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <sm/hash_id.hpp>
#include <map-api/hub.h>

static constexpr int kDefaultIDPrintLength = 10;

namespace map_api {
template <typename IdType>
class UniqueId;
}  // namespace map_api

#define UNIQUE_ID_DEFINE_ID(TypeName)                                    \
  class TypeName : public map_api::UniqueId<TypeName> {                  \
   public:                                                               \
    TypeName() = default;                                                \
    inline TypeName(                                                     \
        const google::protobuf::RepeatedField<uint64_t>& repeated_field) \
        : map_api::UniqueId<TypeName>(repeated_field) {}                 \
    inline TypeName(                                                     \
        const google::protobuf::RepeatedField<uint64_t>& repeated_field, \
        int index)                                                       \
        : map_api::UniqueId<TypeName>(repeated_field, index) {}          \
  };                                                                     \
  extern void defineId##__FILE__##__LINE__(void)
#define UNIQUE_ID_DEFINE_IMMUTABLE_ID(TypeName, BaseTypeName)            \
  class TypeName : public map_api::UniqueId<TypeName> {                  \
   public:                                                               \
    TypeName() = default;                                                \
    inline TypeName(                                                     \
        const google::protobuf::RepeatedField<uint64_t>& repeated_field) \
        : map_api::UniqueId<TypeName>(repeated_field) {}                 \
    inline TypeName(                                                     \
        const google::protobuf::RepeatedField<uint64_t>& repeated_field, \
        int index)                                                       \
        : map_api::UniqueId<TypeName>(repeated_field, index) {}          \
    inline void from##BaseTypeName(const BaseTypeName& landmark_id) {    \
      sm::HashId hash_id;                                                \
      landmark_id.toHashId(&hash_id);                                    \
      this->fromHashId(hash_id);                                         \
    }                                                                    \
  };                                                                     \
  extern void defineId##__FILE__##__LINE__(void)
// this macro needs to be called outside of any namespace
#define UNIQUE_ID_DEFINE_ID_HASH(TypeName)                      \
  namespace std {                                               \
  template <>                                                   \
  struct hash<TypeName> {                                       \
    typedef TypeName argument_type;                             \
    typedef std::size_t value_type;                             \
    value_type operator()(const argument_type& hash_id) const { \
      return hash_id.hashToSizeT();                             \
    }                                                           \
  };                                                            \
  }                                                             \
  extern void defineId##__FILE__##__LINE__(void)

namespace map_api {
template <typename IdType>
void generateId(IdType* id) {
  CHECK_NOTNULL(id);
  id->fromHexString(internal::generateUniqueHexString());
}

template <typename IdType>
IdType createRandomId() {
  IdType id;
  generateId(&id);
  return id;
}

template <typename IdType>
void generateIdFromInt(unsigned int idx, IdType* id) {
  CHECK_NOTNULL(id);
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(32) << idx;
  id->fromHexString(ss.str());
}

// For database internal use only.
class Id : public sm::HashId {
 public:
  Id() = default;
  explicit inline Id(
      const google::protobuf::RepeatedField<uint64_t>& repeated_field) {
    deserialize(repeated_field);
  }
  // For proto fields storing multiple ids.
  inline Id(const google::protobuf::RepeatedField<uint64_t>& repeated_field,
            int index) {
    deserialize(repeated_field, index);
  }
  inline void deserialize(
      const google::protobuf::RepeatedField<uint64_t>& repeated_field) {
    CHECK_EQ(2, repeated_field.size());
    fromUint64(repeated_field.data());
  }
  // For proto fields storing multiple ids.
  inline void deserialize(
      const google::protobuf::RepeatedField<uint64_t>& repeated_field,
      int index) {
    CHECK_EQ(0, index % 2);
    fromUint64(&repeated_field.data()[index]);
  }
  inline void serialize(
      google::protobuf::RepeatedField<uint64_t>* repeated_field) const {
    CHECK_NOTNULL(repeated_field)->Clear();
    repeated_field->Add();
    repeated_field->Add();
    toUint64(repeated_field->mutable_data());
  }
  inline void append(google::protobuf::RepeatedField<uint64_t>* repeated_field)
      const {
    CHECK_NOTNULL(repeated_field);
    int old_size = repeated_field->size();
    CHECK_EQ(0, old_size % 2);
    repeated_field->Add();
    repeated_field->Add();
    toUint64(&repeated_field->mutable_data()[old_size]);
  }
  inline void fromHashId(const sm::HashId& id) {
    static_cast<sm::HashId&>(*this) = id;
  }
  inline void toHashId(sm::HashId* id) const {
    CHECK_NOTNULL(id);
    *id = static_cast<const sm::HashId&>(*this);
  }
  template <typename IdType>
  inline IdType toIdType() const {
    IdType value;
    value.fromHashId(*this);
    return value;
  }
  template <typename GenerateIdType>
  friend void generateId(GenerateIdType* id);

 private:
  using sm::HashId::fromUint64;
  using sm::HashId::toUint64;
};

// To be used for general IDs.
template <typename IdType>
class UniqueId : private Id {
 public:
  UniqueId() = default;
  inline UniqueId(
      const google::protobuf::RepeatedField<uint64_t>& repeated_field)
      : Id(repeated_field) {}
  inline UniqueId(
      const google::protobuf::RepeatedField<uint64_t>& repeated_field,
      int index)
      : Id(repeated_field, index) {}

  using sm::HashId::hexString;
  using sm::HashId::fromHexString;
  using sm::HashId::hashToSizeT;
  using sm::HashId::isValid;
  using sm::HashId::setInvalid;

  using Id::deserialize;
  using Id::serialize;
  using Id::append;

  std::ostream& operator<<(std::ostream& os) const {
    return os << hexString().substr(0, kDefaultIDPrintLength);
  }

  inline void fromHashId(const sm::HashId& id) {
    static_cast<sm::HashId&>(*this) = id;
  }

  inline void toHashId(sm::HashId* id) const {
    CHECK_NOTNULL(id);
    *id = static_cast<const sm::HashId&>(*this);
  }

  inline bool operator==(const IdType& other) const {
    return sm::HashId::operator==(other);
  }

  inline bool operator==(const Id& other) const {
    return sm::HashId::operator==(other);
  }

  inline bool operator!=(const IdType& other) const {
    return sm::HashId::operator!=(other);
  }

  inline bool operator!=(const Id& other) const {
    return sm::HashId::operator!=(other);
  }

  inline bool operator<(const IdType& other) const {
    return sm::HashId::operator<(other);
  }

  inline bool operator<(const Id& other) const {
    return sm::HashId::operator<(other);
  }

  template <typename GenerateIdType>
  friend void generateId(GenerateIdType* id);
};

UNIQUE_ID_DEFINE_ID(ResourceId);
UNIQUE_ID_DEFINE_IMMUTABLE_ID(GlobalResourceId, ResourceId);

}  // namespace map_api

UNIQUE_ID_DEFINE_ID_HASH(map_api::ResourceId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::GlobalResourceId);

namespace std {
inline ostream& operator<<(ostream& out, const map_api::Id& hash) {
  out << hash.hexString().substr(0, kDefaultIDPrintLength);
  return out;
}

template <typename IdType>
inline ostream& operator<<(ostream& out,
                           const map_api::UniqueId<IdType>& hash) {
  out << hash.hexString().substr(0, kDefaultIDPrintLength);
  return out;
}

template <>
struct hash<map_api::Id> {
  std::size_t operator()(const map_api::Id& hashId) const {
    return std::hash<std::string>()(hashId.hexString());
  }
};
}  // namespace std

#endif  // MAP_API_UNIQUE_ID_H_
