#ifndef INTERNAL_COMBINED_VIEW_H_
#define INTERNAL_COMBINED_VIEW_H_

#include "map-api/internal/overriding-view-base.h"

namespace map_api {
namespace internal {

class CombinedView : public ViewBase {
 public:
  CombinedView(const ViewBase& complete_view,
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

 private:
  const ViewBase& complete_view_;
  const OverridingViewBase& override_view_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMBINED_VIEW_H_
