#ifndef SIMPLE_TABLE_H_
#define SIMPLE_TABLE_H_

#include <string>

#include "map-api/cr-table.h"

namespace map_api {

template<typename TableType>
class SimpleTableIntermediate;

// FIXME(tcies) instead, add defineCustomFields method to CRTable
template<>
class SimpleTableIntermediate<CRTable> : public CRTable {
  virtual void defineFieldsCRDerived() {
    defineCustomFields();
  }
  virtual void defineCustomFields() = 0;
};
template<>
class SimpleTableIntermediate<CRUTable> : public CRUTable {
  virtual void defineFieldsCRUDerived() {
    defineCustomFields();
  }
  virtual void defineCustomFields() = 0;
};

/**
 * Defines a table interface of the desired type containing only one field,
 * of type DataType, with name "data" (but use the constant).
 * REVISION_SET, _GET and _ENUM must of course be defined separately
 */
template<typename TableType, typename DataType>
class SimpleTable : public SimpleTableIntermediate<TableType> {
 public:
  static const std::string kDataField;
  /**
   * This still needs to be implemented - unfortunately, cpp doesn't support
   * passing string literals through templates.
   */
  virtual const std::string name() const override;
  virtual void defineCustomFields() {
    this->template addField<DataType>(kDataField);
  }
  static SimpleTable<TableType, DataType>& instance();
 protected:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS_DIRECT(SimpleTable);
};

template<typename TableType, typename DataType>
const std::string SimpleTable<TableType, DataType>::kDataField =
    "data_test";

template<typename TableType, typename DataType>
SimpleTable<TableType, DataType>& SimpleTable<TableType, DataType>::instance() {
  static SimpleTable<TableType, DataType> object;
  return object;
}


} // namespace map_api

#endif /* SIMPLE_TABLE_H_ */
