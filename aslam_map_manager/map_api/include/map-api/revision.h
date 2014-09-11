#ifndef MAP_API_REVISION_H_
#define MAP_API_REVISION_H_

#include <memory>
#include <unordered_map>
#include <set>
#include <string>

#include <glog/logging.h>
#include <Poco/Data/BLOB.h>
#include <Poco/Data/Statement.h>

#include "map-api/logical-time.h"
#include "map-api/unique-id.h"
#include "./core.pb.h"

namespace map_api {

class Revision {
  friend class Chunk;
  friend class CRTable;
  friend class CRUTable;

 public:
  explicit Revision(const std::shared_ptr<proto::Revision>& revision);
  /**
   * Insert placeholder in SQLite insert statements. Returns blob shared pointer
   * for dynamically created blob objects
   */
  std::shared_ptr<Poco::Data::BLOB> insertPlaceHolder(
      int index, Poco::Data::Statement* stat) const;

  template <typename FieldType>
  static proto::Type getProtobufTypeEnum();

  void addField(int index, const proto::Type& type);
  template <typename FieldType>
  void addField(int index);

  template <typename FieldType>
  bool set(int index, const FieldType& value);

  template <typename FieldType>
  bool get(int index, FieldType* value) const;

  inline LogicalTime getInsertTime() const {
    return LogicalTime(underlying_revision_->insert_time());
  }
  inline LogicalTime getUpdateTime() const {
    return LogicalTime(underlying_revision_->update_time());
  }
  inline LogicalTime getModificationTime() const {
    return (underlying_revision_->has_update_time()) ? getUpdateTime()
                                                     : getInsertTime();
  }
  inline Id getChunkId() const {
    Id id;
    id.fromHexString(underlying_revision_->chunk_id().hash());
    return id;
  }
  inline Id getId() const {
    Id id;
    id.fromHexString(underlying_revision_->id().hash());
    return id;
  }
  inline bool isRemoved() const { return underlying_revision_->has_removed(); }

  template <typename ExpectedType>
  bool verifyEqual(int index, const ExpectedType& expected) const;

  /**
   * Returns true if Revision contains same fields as other
   */
  bool structureMatch(const Revision& other) const;

  /**
   * Returns true if value at key is same as with other
   */
  bool fieldMatch(const Revision& other, int index) const;

  std::string dumpToString() const;

  inline std::string serializeUnderlying() {
    return underlying_revision_->SerializeAsString();
  }

  inline int byteSize() const { return underlying_revision_->ByteSize(); }

 private:
  inline void setInsertTime(const LogicalTime& time) {
    underlying_revision_->set_insert_time(time.serialize());
  }
  inline void setUpdateTime(const LogicalTime& time) {
    underlying_revision_->set_update_time(time.serialize());
  }
  inline void setChunkId(const Id& id) {  // TODO(tcies) mutable, zerocopy
    underlying_revision_->mutable_chunk_id()->set_hash(id.hexString());
  }
  inline void setRemoved() { underlying_revision_->set_removed(true); }

  template <typename FieldType>
  bool set(const FieldType& value, proto::TableField* field);

  template <typename FieldType>
  bool get(const proto::TableField& field, FieldType* value) const;

  std::shared_ptr<proto::Revision> underlying_revision_;
};

/**
 * Convenience macros to specialize the above templates in one line.
 */
#define MAP_API_TYPE_ENUM(TYPE, ENUM)                 \
  template <>                                         \
  proto::Type Revision::getProtobufTypeEnum<TYPE>() { \
    return ENUM;                                      \
  }                                                   \
  extern void revEnum##__FILE__##__LINE__(void)

#define MAP_API_REVISION_SET(TYPE) \
  template <>                      \
  bool Revision::set<TYPE>(const TYPE& value, proto::TableField* field)

#define MAP_API_REVISION_GET(TYPE) \
  template <>                      \
  bool Revision::get<TYPE>(const proto::TableField& field, TYPE* value) const

/**
 * One Macro to define REVISION_ENUM, _SET and _GET for Protobuf objects
 */
#define MAP_API_REVISION_PROTOBUF(TYPE)                                      \
  MAP_API_TYPE_ENUM(TYPE, ::map_api::proto::TableFieldDescriptor_Type_BLOB); \
                                                                             \
  MAP_API_REVISION_SET(TYPE) {                                               \
    CHECK_NOTNULL(field)->set_blobvalue(value.SerializeAsString());          \
    return true;                                                             \
  }                                                                          \
                                                                             \
  MAP_API_REVISION_GET(TYPE) {                                               \
    CHECK_NOTNULL(value);                                                    \
    bool parsed = value->ParseFromString(field.blobvalue());                 \
    if (!parsed) {                                                           \
      LOG(ERROR) << "Failed to parse " << #TYPE;                             \
      return false;                                                          \
    }                                                                        \
    return true;                                                             \
  }                                                                          \
  extern void __FILE__##__LINE__(void)
/**
 * Same for UniqueId derivates
 */
#define MAP_API_REVISION_UNIQUE_ID(TypeName)                              \
  MAP_API_TYPE_ENUM(TypeName,                                             \
                    ::map_api::proto::TableFieldDescriptor_Type_HASH128); \
  MAP_API_REVISION_SET(TypeName) {                                        \
    CHECK_NOTNULL(field)->set_stringvalue(value.hexString());             \
    return true;                                                          \
  }                                                                       \
  MAP_API_REVISION_GET(TypeName) {                                        \
    return CHECK_NOTNULL(value)->fromHexString(field.stringvalue());      \
  }                                                                       \
  extern void __FILE__##__LINE__(void)

/**
 * A generic, blob-y field type for testing blob insertion
 */
class testBlob : public map_api::proto::TableField {
 public:
  inline bool operator==(const testBlob& other) const {
    if (!this->has_type()) return !other.has_type();
    return type() == other.type();
  }
};

}  // namespace map_api

#include "map-api/revision-inl.h"

#endif  // MAP_API_REVISION_H_
