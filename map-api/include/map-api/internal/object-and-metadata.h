#ifndef INTERNAL_OBJECT_AND_METADATA_H_
#define INTERNAL_OBJECT_AND_METADATA_H_

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
  }
};

}  // namespace map_api

#endif  // INTERNAL_OBJECT_AND_METADATA_H_
