#ifndef MAP_API_CHUNK_MANAGER_H_
#define MAP_API_CHUNK_MANAGER_H_
#include <unordered_map>
#include <utility>

#include "./net-table.pb.h"
#include <map-api/chunk.h>
#include <map-api/net-table.h>

namespace map_api {
class ChunkManagerBase {
 public:
  explicit ChunkManagerBase(map_api::NetTable* underlying_table)
      : underlying_table_(CHECK_NOTNULL(underlying_table)) {}

  virtual ~ChunkManagerBase() {}

  // Returns the chunk in which the given item can be placed.
  virtual Chunk* getChunkForItem(const Revision& revision) = 0;

  inline map_api::NetTable* getUnderlyingTable() {
    return underlying_table_;
  };

  inline size_t numChunks() const { return active_chunks_.size(); }

  inline void getChunkIds(IdSet* chunk_ids) const {
    CHECK_NOTNULL(chunk_ids);
    chunk_ids->clear();
    chunk_ids->rehash(active_chunks_.size());
    for (const std::pair<const Id, Chunk*>& pair : active_chunks_) {
      chunk_ids->emplace(pair.first);
    }
  }
  inline void getChunkIds(proto::ChunkIdList* chunk_id_list) const {
    CHECK_NOTNULL(chunk_id_list);
    chunk_id_list->clear_chunk_ids();
    for (const std::pair<const Id, Chunk*>& pair : active_chunks_) {
      pair.first.serialize(chunk_id_list->add_chunk_ids());
    }
  }

  void requestParticipationAllChunks();

 protected:
  map_api::NetTable* underlying_table_;
  std::unordered_map<Id, Chunk*> active_chunks_;
};

static constexpr int kDefaultChunkSizeBytes = 2 * 1024 * 1024;
// A Chunk manager that splits chunks based on their size.
class ChunkManagerChunkSize : public ChunkManagerBase {
 public:
  ChunkManagerChunkSize(int max_chunk_size_bytes,
                        map_api::NetTable* underlying_table)
      : ChunkManagerBase(CHECK_NOTNULL(underlying_table)),
        max_chunk_size_bytes_(max_chunk_size_bytes),
        current_chunk_(nullptr),
        current_chunk_size_bytes_(0) {}
  ~ChunkManagerChunkSize() {}

  virtual Chunk* getChunkForItem(const Revision& revision);

 private:
  int max_chunk_size_bytes_;
  Chunk* current_chunk_;
  int current_chunk_size_bytes_;
};

}  // namespace map_api
#endif  // MAP_API_CHUNK_MANAGER_H_
