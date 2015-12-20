#ifndef INTERNAL_DELTA_VIEW_H_
#define INTERNAL_DELTA_VIEW_H_

#include "map-api/internal/overriding-view-base.h"
#include "map-api/revision-map.h"

namespace map_api {
namespace internal {

class DeltaView : public OverridingViewBase {
 public:
  ~DeltaView();
  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const common::Id& id) const
      override;
  // ============================
  // OVERRIDINGVIEWBASE INTERFACE
  // ============================
  virtual bool supresses(const common::Id& id) const override;

  // ===============
  // DELTA INTERFACE
  // ===============
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);

 private:
  // Strong typing of operation maps.
  class InsertMap : public MutableRevisionMap {};
  class UpdateMap : public MutableRevisionMap {};
  class RemoveMap : public MutableRevisionMap {};

  InsertMap insertions_;
  UpdateMap updates_;
  RemoveMap removes_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_DELTA_VIEW_H_
