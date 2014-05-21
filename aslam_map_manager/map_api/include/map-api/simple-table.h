#ifndef SIMPLE_TABLE_H_
#define SIMPLE_TABLE_H_

/**
 * Defines a table interface of the desired type containing only one field,
 * of type DataType, with name "data" (but use the constant).
 * REVISION_SET, _GET and _ENUM must of course be defined separately
 */
template<typename TableType, typename DataType, std::string TableName>
class SimpleTable : public TableType {
 public:
  static const std::string kDataField = "data";
  virtual const std::string name() const override {
    return TableName;
  }
  virtual ~SimpleTable() {}
 protected:
  virtual void define() {
    TableType::addField<TableType>(kDataField);
  }
};

template<typename TableType, typename DataType, std::string TableName>
const std::string SimpleTable<TableType, DataType, TableName>::kDataField =
    "data_test";


#endif /* SIMPLE_TABLE_H_ */
