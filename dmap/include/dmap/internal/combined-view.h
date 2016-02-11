#ifndef INTERNAL_COMBINED_VIEW_H_
#define INTERNAL_COMBINED_VIEW_H_

#include "dmap/internal/overriding-view-base.h"

namespace dmap {
namespace internal {

class CombinedView : public ViewBase {
 public:
  CombinedView(const std::unique_ptr<ViewBase>& complete_view,
               const OverridingViewBase& override_view);
  ~CombinedView();

  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  // Using unique_ptr because the complete view can be swapped out.
  const std::unique_ptr<ViewBase>& complete_view_;
  const OverridingViewBase& override_view_;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_COMBINED_VIEW_H_
