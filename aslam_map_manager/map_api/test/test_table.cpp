#include <map-api/cru-table-interface.h>
#include <glog/logging.h>

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
template <typename TableInterfaceType>
class TestTable : public TableInterfaceType {
 public:
  ~TestTable() {}
  virtual const std::string name() const override {
    return "test_table";
  }
  std::shared_ptr<Poco::Data::Session> sessionForward(){
    return std::shared_ptr<Poco::Data::Session>(this->session_);
  }
  void cleanup(){
    *(sessionForward()) << "DROP TABLE IF EXISTS " << this->name(),
        Poco::Data::now;
    LOG(INFO) << "Table " << this->name() << " dropped";
  }
 protected:
  virtual void define(){}

 public:
  using TableInterfaceType::rawInsertQuery;
  using TableInterfaceType::rawGetRow;
};
