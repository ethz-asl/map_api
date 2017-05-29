#ifndef DMAP_TABLE_DESCRIPTOR_INL_H_
#define DMAP_TABLE_DESCRIPTOR_INL_H_

#include "map-api/revision.h"

namespace map_api {

template <typename Type>
void TableDescriptor::addField(int index) {
  addField(index, Revision::getProtobufTypeEnum<Type>());
}

}  // namespace map_api

#endif  // DMAP_TABLE_DESCRIPTOR_INL_H_
