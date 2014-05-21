#ifndef SIMPLE_TABLE_H_
#define SIMPLE_TABLE_H_

#include <string>

namespace map_api {

/**
 * Defines a table interface of the desired type containing only one field,
 * of type DataType, with name "data" (but use the constant).
 * REVISION_SET, _GET and _ENUM must of course be defined separately
 */
template<typename TableType, typename DataType>
class SimpleTable : public TableType {
 public:
  static const std::string kDataField;
  /**
   * This still needs to be implemented - unfortunately, cpp doesn't support
   * passing string literals through templates.
   */
  virtual const std::string name() const override;
  virtual ~SimpleTable() {}
 protected:
  virtual void define() {
    this->template addField<DataType>(kDataField);
  }
};

template<typename TableType, typename DataType>
const std::string SimpleTable<TableType, DataType>::kDataField =
    "data_test";

} // namespace map_api

#endif /* SIMPLE_TABLE_H_ */
