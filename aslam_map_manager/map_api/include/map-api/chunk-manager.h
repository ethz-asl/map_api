#ifndef MAP_API_CHUNK_MANAGER_H_
#define MAP_API_CHUNK_MANAGER_H_
#include <unordered_map>
#include <utility>

#include "map-api/chunk.h"
#include "map-api/net-table.h"

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

  void requestParticipationAllChunks();

 protected:
  map_api::NetTable* underlying_table_;
  std::unordered_map<Id, Chunk*> active_chunks_;
};

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
