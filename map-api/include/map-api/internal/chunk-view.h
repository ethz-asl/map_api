#ifndef INTERNAL_CHUNK_VIEW_H_
#define INTERNAL_CHUNK_VIEW_H_

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
  virtual bool has(const common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const common::Id& id) const
      override;

 private:
  const ChunkBase& chunk_;
  const LogicalTime view_time_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_CHUNK_VIEW_H_
