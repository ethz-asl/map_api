#ifndef NET_CR_TABLE_H_
#define NET_CR_TABLE_H_

#include <unordered_map>

#include "map-api/chunk-manager.h"
#include "map-api/cr-table.h"
#include "map-api/cr-table-ram-cache.h"
#include "map-api/revision.h"

namespace map_api {

class NetCRTable {
 public:
  static const std::string kChunkIdField;

  bool init(std::unique_ptr<TableDescriptor>* descriptor);
  const std::string& name() const;

  // INSERTION
  std::shared_ptr<Revision> getTemplate() const;
  std::weak_ptr<Chunk> newChunk() const;
  bool insert(const std::weak_ptr<Chunk>& chunk, Revision* query);

  // RETRIEVAL
  std::shared_ptr<Revision> getById(const Id& id, const Time& time);
  /**
   * Finding: If can't find item locally, request at peers. There are subtleties
   * here: Is it enough to get data only from one chunk? I.e. shouldn't we
   * theoretically request data from all peers, even if we found some matching
   * items locally? Yes, we should - this would be horribly inefficient though.
   * Thus it would probably be better to expose two different
   * functions in the Net-CR-table: FastFind and ThoroughFind
   * (and of course FindUnique, which is a special case of FastFind). FastFind
   * would then only look until results from only one chunk have been found -
   * the chunk possibly already being held.
   * For the time being implementing only FastFind for simplicity.
   */
  template<typename ValueType>
  int findFast(
      const std::string& key, const ValueType& value, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination);
  int findFastByRevision(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination) final;
  template<typename ValueType>
  std::shared_ptr<Revision> findUnique(
      const std::string& key, const ValueType& value, const Time& time);
  void dumpCache(
      const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination);

 private:
  std::unique_ptr<CRTableRAMCache> cache_;
  std::unique_ptr<ChunkManager> chunk_manager_;
  // TODO(tcies) insert PeerHandler here
};

} // namespace map_api

#endif /* NET_CR_TABLE_H_ */
