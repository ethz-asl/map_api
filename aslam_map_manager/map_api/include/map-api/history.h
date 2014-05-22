#ifndef HISTORY_H_
#define HISTORY_H_

#include <map-api/cr-table-interface.h>
#include <map-api/time.h>

namespace map_api {

class History final : public CRTableInterface {
 public:
  /**
   * Field names
   */
  static const std::string kPreviousField;
  static const std::string kRevisionField;
  static const std::string kRevisionTimeField;
  /**
   * Define the table of which the history is to be kept.
   * Will create a table with the name <tableName>_history.
   */
  explicit History(const std::string& table_name);
  /**
   * Takes the table name taken from constructor to set up table interface
   */
  virtual const std::string name() const override;

 private:
  virtual void define() override;
  std::string table_name_;
};

} /* namespace map_api */

#endif /* HISTORY_H_ */
