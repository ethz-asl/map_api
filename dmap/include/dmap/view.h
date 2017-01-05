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

protected:
  std::shared_ptr<Transaction> transaction_;
};

} // namespace dmap
