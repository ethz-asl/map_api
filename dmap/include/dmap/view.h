#pragma once

#include <memory>

namespace dmap
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

} // namespace dmap
