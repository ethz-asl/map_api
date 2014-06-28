#ifndef MAP_API_CHUNK_TRANSACTION_H_
#define MAP_API_CHUNK_TRANSACTION_H_

#include <memory>
#include <unordered_map>

#include "map-api/cr-table.h"
#include "map-api/id.h"
#include "map-api/revision.h"
#include "map-api/time.h"

namespace map_api {

/**
 * This class is somewhat weaker than the first transaction draft
 * (LocalTransaction, now deprecated) because conflict checking and
 * committing is handled in the Chunk class.
 */
class ChunkTransaction {
 public:
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  // TODO(tcies) conflict conditions
  std::shared_ptr<Revision> getById(const Id& id);
  std::shared_ptr<Revision> getByIdFromUncommitted(const Id& id) const;
  // TODO(tcies) all other flavors of reading

 private:
  ChunkTransaction(const Time& begin_time, CRTable* cache);
  ChunkTransaction(const ChunkTransaction&) = delete;
  ChunkTransaction& operator =(const ChunkTransaction&) = delete;
  friend class Chunk;

  /**
   * Strong typing of table operation maps.
   */
  class InsertMap : public std::unordered_map<Id, std::shared_ptr<Revision> > {
  };
  class UpdateMap : public std::unordered_map<Id, std::shared_ptr<Revision> > {
  };
  InsertMap insertions_;
  UpdateMap updates_;
  Time begin_time_;
  CRTable* cache_;
  std::shared_ptr<const Revision> structure_reference_;
};

} /* namespace map_api */

#endif /* MAP_API_CHUNK_TRANSACTION_H_ */
