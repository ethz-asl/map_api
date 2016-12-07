#include "dmap/view.h"

#include "dmap/transaction.h"

namespace dmap
{

View::View() : transaction_(new Transaction)
{

}

} // namespace dmap
