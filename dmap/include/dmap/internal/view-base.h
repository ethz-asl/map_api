#ifndef INTERNAL_VIEW_BASE_H_
#define INTERNAL_VIEW_BASE_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace dmap_common {
class Id;
}  // namespace common

namespace dmap {
class ConstRevisionMap;
class LogicalTime;
class Revision;

namespace internal {

class ViewBase {
 public:
  virtual ~ViewBase();

  virtual bool has(const dmap_common::Id& id) const = 0;
  virtual std::shared_ptr<const Revision> get(const dmap_common::Id& id) const = 0;
  virtual void dump(ConstRevisionMap* result) const = 0;
  virtual void getAvailableIds(std::unordered_set<dmap_common::Id>* result)
      const = 0;

  // From the supplied update times, discard the ones whose updates are
  // incorporated by the view.
  typedef std::unordered_map<common::Id, LogicalTime> UpdateTimes;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const = 0;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_VIEW_BASE_H_
