#ifndef DMAP_TABLE_DESCRIPTOR_INL_H_
#define DMAP_TABLE_DESCRIPTOR_INL_H_

#include "dmap/revision.h"

namespace dmap {

template <typename Type>
void TableDescriptor::addField(int index) {
  addField(index, Revision::getProtobufTypeEnum<Type>());
}

}  // namespace dmap

#endif  // DMAP_TABLE_DESCRIPTOR_INL_H_
