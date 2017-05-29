#ifndef INTERNAL_CHUNK_VIEW_H_
#define INTERNAL_CHUNK_VIEW_H_

#include <unordered_map>

#include "map-api/internal/view-base.h"
#include "map-api/logical-time.h"

namespace map_api {
class ChunkBase;

namespace internal {

class ChunkView : public ViewBase {
 public:
  ChunkView(const ChunkBase& chunk, const LogicalTime& view_time);
  ~ChunkView();

  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const map_api_common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const map_api_common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<map_api_common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  const ChunkBase& chunk_;
  const LogicalTime view_time_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_CHUNK_VIEW_H_
