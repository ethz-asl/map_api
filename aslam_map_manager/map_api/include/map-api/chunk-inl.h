#ifndef MAP_API_CHUNK_INL_H_
#define MAP_API_CHUNK_INL_H_

namespace map_api {

template <typename RequestType>
void Chunk::fillMetadata(RequestType* destination) {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->underlying_table_->name());
  destination->mutable_metadata()->set_chunk_id(id().hexString());
}

} // namespace map_api

#endif /* MAP_API_CHUNK_INL_H_ */
