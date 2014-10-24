#ifndef MAP_API_TABLE_DESCRIPTOR_INL_H_
#define MAP_API_TABLE_DESCRIPTOR_INL_H_

namespace map_api {

template <typename Type>
void TableDescriptor::addField(int index) {
  addField(index, Revision::getProtobufTypeEnum<Type>());
}

}  // namespace map_api

#endif  // MAP_API_TABLE_DESCRIPTOR_INL_H_
