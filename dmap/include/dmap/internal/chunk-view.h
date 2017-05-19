#ifndef INTERNAL_CHUNK_VIEW_H_
#define INTERNAL_CHUNK_VIEW_H_

#include <unordered_map>

#include "dmap/internal/view-base.h"
#include "dmap/logical-time.h"

namespace dmap {
class ChunkBase;

namespace internal {

class ChunkView : public ViewBase {
 public:
  ChunkView(const ChunkBase& chunk, const LogicalTime& view_time);
  ~ChunkView();

  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const dmap_common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const dmap_common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<dmap_common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  const ChunkBase& chunk_;
  const LogicalTime view_time_;
};

}  // namespace internal
}  // namespace dmap

#endif  // INTERNAL_CHUNK_VIEW_H_
