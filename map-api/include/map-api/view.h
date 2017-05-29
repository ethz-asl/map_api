#pragma once

#include <memory>

namespace map_api
{
class Transaction;

class View
{
public:
  View();

  virtual ~View() = default;

  bool commit();

protected:
  std::shared_ptr<Transaction> transaction_;

private:
  virtual void overrideTrackerIdentificationMethods();
};

} // namespace map_api
