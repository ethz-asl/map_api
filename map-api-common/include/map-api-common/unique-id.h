// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_COMMON_UNIQUE_ID_H_
#define MAP_API_COMMON_UNIQUE_ID_H_

#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "map-api-common/hash-id.h"
#include "map-api-common/internal/unique-id.h"

static constexpr int kDefaultIDPrintLength = 10;

namespace map_api {
class NetTable;
class NetTableTransaction;
}  // namespace map_api

namespace map_api_common {
template <typename IdType>
class UniqueId;
}  // namespace map_api_common

#define UNIQUE_ID_DEFINE_ID(TypeName)                           \
  class TypeName : public map_api_common::UniqueId<TypeName> {          \
   public:                                                      \
    TypeName() = default;                                       \
    explicit inline TypeName(const map_api_common::proto::Id& id_field) \
        : map_api_common::UniqueId<TypeName>(id_field) {}               \
  };                                                            \
  typedef std::vector<TypeName> TypeName##List;                 \
  typedef std::unordered_set<TypeName> TypeName##Set;           \
  extern void defineId##__FILE__##__LINE__(void)

#define UNIQUE_ID_DEFINE_IMMUTABLE_ID(TypeName, BaseTypeName)         \
  class TypeName : public map_api_common::UniqueId<TypeName> {                \
   public:                                                            \
    TypeName() = default;                                             \
    explicit inline TypeName(const map_api_common::proto::Id& id_field)       \
        : map_api_common::UniqueId<TypeName>(id_field) {}                     \
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

namespace map_api_common {
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

// For Map API internal use only.
class Id : public HashId {
 public:
  Id() = default;
  explicit inline Id(const map_api_common::proto::Id& id_field) {
    deserialize(id_field);
  }
  inline void deserialize(const map_api_common::proto::Id& id_field) {
    CHECK_EQ(id_field.uint_size(), 2);
    fromUint64(id_field.uint().data());
  }
  inline void serialize(map_api_common::proto::Id* id_field) const {
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

  bool correspondsTo(const map_api_common::proto::Id& proto_id) const {
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
  explicit inline UniqueId(const map_api_common::proto::Id& id_field) : Id(id_field) {}

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

  // Making base type accessible to select Map API classes.
  friend class map_api::NetTable;
  friend class map_api::NetTableTransaction;
};
}  // namespace map_api_common

namespace std {
inline ostream& operator<<(ostream& out, const map_api_common::Id& hash) {
  out << hash.printString();
  return out;
}

template <typename IdType>
inline ostream& operator<<(ostream& out,
                           const map_api_common::UniqueId<IdType>& hash) {
  out << hash.printString();
  return out;
}

template <typename IdType>
inline istream& operator>>(istream& is, map_api_common::UniqueId<IdType>& id) {
  std::string hex_string;
  is >> hex_string;
  id.fromHexString(hex_string);
  return is;
}

template <>
struct hash<map_api_common::Id> {
  std::size_t operator()(const map_api_common::Id& hashId) const {
    return std::hash<std::string>()(hashId.hexString());
  }
};
}  // namespace std

#endif  // MAP_API_COMMON_UNIQUE_ID_H_
