#include "dmap/view.h"

#include "dmap/transaction.h"

namespace dmap
{

View::View() : transaction_(new Transaction)
{
}

bool View::commit()
{
  overrideTrackerIdentificationMethods();
  return transaction_->commit();
}

void View::overrideTrackerIdentificationMethods()
{

}

} // namespace dmap
