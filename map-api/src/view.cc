#include "map-api/view.h"

#include "map-api/transaction.h"

namespace map_api
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

} // namespace map_api
