#ifndef DMAP_COMMON_UNIQUE_ID_H_
#define DMAP_COMMON_UNIQUE_ID_H_

#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "dmap-common/hash-id.h"
#include "dmap-common/internal/unique-id.h"

static constexpr int kDefaultIDPrintLength = 10;

namespace dmap {
class NetTable;
class NetTableTransaction;
}  // namespace dmap

namespace dmap_common {
template <typename IdType>
class UniqueId;
}  // namespace dmap_common

#define UNIQUE_ID_DEFINE_ID(TypeName)                           \
  class TypeName : public dmap_common::UniqueId<TypeName> {          \
   public:                                                      \
    TypeName() = default;                                       \
    explicit inline TypeName(const dmap_common::proto::Id& id_field) \
        : dmap_common::UniqueId<TypeName>(id_field) {}               \
  };                                                            \
  typedef std::vector<TypeName> TypeName##List;                 \
  typedef std::unordered_set<TypeName> TypeName##Set;           \
  extern void defineId##__FILE__##__LINE__(void)

#define UNIQUE_ID_DEFINE_IMMUTABLE_ID(TypeName, BaseTypeName)         \
  class TypeName : public dmap_common::UniqueId<TypeName> {                \
   public:                                                            \
    TypeName() = default;                                             \
    explicit inline TypeName(const dmap_common::proto::Id& id_field)       \
        : dmap_common::UniqueId<TypeName>(id_field) {}                     \
    inline void from##BaseTypeName(const BaseTypeName& landmark_id) { \
      HashId hash_id;                                          \
      landmark_id.toHashId(&hash_id);                                 \
      this->fromHashId(hash_id);                                      \
    }                                                                 \
  };                                                                  \
  typedef std::vector<TypeName> TypeName##List;                       \
  typedef std::unordered_set<TypeName> TypeName##Set;                 \
  extern void defineId##__FILE__##__LINE__(void)

// This macro needs to be called outside of any namespace.
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

namespace dmap_common {
template <typename IdType>
void generateId(IdType* id) {
  CHECK_NOTNULL(id);
  uint64_t hash[2];
  internal::generateUnique128BitHash(hash);
  id->fromUint64(hash);
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

// For dmap internal use only.
class Id : public HashId {
 public:
  Id() = default;
  explicit inline Id(const dmap_common::proto::Id& id_field) {
    deserialize(id_field);
  }
  inline void deserialize(const dmap_common::proto::Id& id_field) {
    CHECK_EQ(id_field.uint_size(), 2);
    fromUint64(id_field.uint().data());
  }
  inline void serialize(dmap_common::proto::Id* id_field) const {
    CHECK_NOTNULL(id_field)->clear_uint();
    id_field->mutable_uint()->Add();
    id_field->mutable_uint()->Add();
    toUint64(id_field->mutable_uint()->mutable_data());
  }

  inline void fromHashId(const HashId& id) {
    static_cast<HashId&>(*this) = id;
  }
  inline void toHashId(HashId* id) const {
    CHECK_NOTNULL(id);
    *id = static_cast<const HashId&>(*this);
  }

  std::string printString() const {
    constexpr int kPartLength = (kDefaultIDPrintLength - 2) / 2;
    const std::string hex = hexString();
    return hex.substr(0, kPartLength) + ".." +
           hex.substr(hex.length() - kPartLength, kPartLength);
  }

  template <typename IdType>
  inline IdType toIdType() const {
    IdType value;
    value.fromHashId(*this);
    return value;
  }
  template <typename GenerateIdType>
  friend void generateId(GenerateIdType* id);

  bool correspondsTo(const dmap_common::proto::Id& proto_id) const {
    Id corresponding(proto_id);
    return operator==(corresponding);
  }

 private:
  using HashId::fromUint64;
  using HashId::toUint64;
};

typedef std::unordered_set<Id> IdSet;
typedef std::vector<Id> IdList;

// To be used for general IDs.
template <typename IdType>
class UniqueId : private Id {
 public:
  UniqueId() = default;
  explicit inline UniqueId(const dmap_common::proto::Id& id_field) : Id(id_field) {}

  using HashId::hexString;
  using HashId::fromHexString;
  using HashId::hashToSizeT;
  using HashId::isValid;
  using HashId::setInvalid;
  using HashId::shortHex;

  using Id::deserialize;
  using Id::printString;
  using Id::serialize;
  using Id::toIdType;

  std::ostream& operator<<(std::ostream& os) const {
    return os << printString();
  }

  inline void fromHashId(const HashId& id) {
    static_cast<HashId&>(*this) = id;
  }

  inline void toHashId(HashId* id) const {
    CHECK_NOTNULL(id);
    *id = static_cast<const HashId&>(*this);
  }

  inline bool operator==(const IdType& other) const {
    return HashId::operator==(other);
  }

  inline bool operator==(const Id& other) const {
    return HashId::operator==(other);
  }

  inline bool operator!=(const IdType& other) const {
    return HashId::operator!=(other);
  }

  inline bool operator!=(const Id& other) const {
    return HashId::operator!=(other);
  }

  inline bool operator<(const IdType& other) const {
    return HashId::operator<(other);
  }

  inline bool operator<(const Id& other) const {
    return HashId::operator<(other);
  }

  template <typename GenerateIdType>
  friend void generateId(GenerateIdType* id);

  // Making base type accessible to select dmap classes.
  friend class dmap::NetTable;
  friend class dmap::NetTableTransaction;
};
}  // namespace dmap_common

namespace std {
inline ostream& operator<<(ostream& out, const dmap_common::Id& hash) {
  out << hash.printString();
  return out;
}

template <typename IdType>
inline ostream& operator<<(ostream& out,
                           const dmap_common::UniqueId<IdType>& hash) {
  out << hash.printString();
  return out;
}

template <typename IdType>
inline istream& operator>>(istream& is, dmap_common::UniqueId<IdType>& id) {
  std::string hex_string;
  is >> hex_string;
  id.fromHexString(hex_string);
  return is;
}

template <>
struct hash<dmap_common::Id> {
  std::size_t operator()(const dmap_common::Id& hashId) const {
    return std::hash<std::string>()(hashId.hexString());
  }
};
}  // namespace std

#endif  // DMAP_COMMON_UNIQUE_ID_H_
