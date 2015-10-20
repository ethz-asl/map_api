#ifndef INTERNAL_OBJECT_AND_METADATA_H_
#define INTERNAL_OBJECT_AND_METADATA_H_

#include "map-api/net-table.h"
#include "map-api/revision.h"

namespace map_api {

// Splits a revision into object and metadata. Note that the revision stored
// in this object contains no custom field values, as these would be redundant
// with the Object; it only contains the metadata.
template <typename ObjectType>
struct ObjectAndMetadata {
  ObjectType object;
  std::shared_ptr<Revision> metadata;

  void deserialize(const Revision& source) {
    objectFromRevision(source, &object);
    source.copyForWrite(&metadata);
    metadata->clearCustomFieldValues();
    VLOG(5) << metadata->dumpToString();
    VLOG(5) << common::backtrace();
    VLOG(5) << this;
  }

  void serialize(std::shared_ptr<const Revision>* destination) const {
    CHECK_NOTNULL(destination);
    std::shared_ptr<Revision> result;
    metadata->copyForWrite(&result);
    objectToRevision(object, result.get());
    *destination = result;
  }

  void createForInsert(const ObjectType& _object, NetTable* table) {
    CHECK_NOTNULL(table);
    object = _object;
    metadata = table->getTemplate();
  }
};

}  // namespace map_api

#endif  // INTERNAL_OBJECT_AND_METADATA_H_
