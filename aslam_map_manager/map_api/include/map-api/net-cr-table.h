#ifndef NET_CR_TABLE_H_
#define NET_CR_TABLE_H_

#include <unordered_map>

#include "map-api/chunk.h"
#include "map-api/cr-table.h"
#include "map-api/revision.h"

namespace map_api {

class NetCRTable : public CRTable {
 public:
  static const std::string kChunkIdField;
  virtual bool init() override;
  virtual void defineFieldsCRDerived() final override;

  /**
   * Functions to be implemented by the derived classes
   */
  virtual const std::string name() const override = 0;
  virtual void defineFieldsNetCRDerived() = 0;
  // Don't forget MEYERS_SINGLETON_INSTANCE_FUNCTION and the protected methods

 protected:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS(NetCRTable);

  /**
   * Insertion: Also sends data to chunk-mates
   */
  bool netInsert(const std::weak_ptr<Chunk>& chunk, Revision* query);
  /**
   * Finding: If can't find item locally, request at peers. There are subtleties
   * here: Is it enough to get data only from one chunk? I.e. shouldn't we
   * theoretically request data from all peers, even if we found some matching
   * items locally? Yes, we should - this would be horribly inefficient though.
   * Thus TODO(tcies) it would probably be better to expose two different
   * functions in the Net-CR-table: For instance, FastFind and ThoroughFind
   * (and of course FindUnique, which is a special case of FastFind). FastFind
   * would then only look until results from only one chunk have been found -
   * the chunk possibly already being held.
   * For the time being implementing FastFind for simplicity.
   */
  int netFindFast(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest);


 private:
  /**
   * Hiding some of CRTable's functions in favor of their net-flavors.
   * Mostly, these consist in not being const any more.
   * TODO(tcies) base table class instead?
   */
  using CRTable::rawInsert; // netInsert
  using CRTable::rawGetById; // netGetById
  using CRTable::rawFind; // netFindFast, netFindThorough
  using CRTable::rawFindByRevision; // TODO(tcies) can we get rid of this?
  using CRTable::rawFindUnique; // netFindUnique
  // keeping rawDump, which now reflects the currently cached table contents

  /**
   * Weak pointers to chunks held by ChunkManager that correspond to this table
   */
  std::unordered_map<Id, std:: weak_ptr<Chunk> > active_chunks_;
  /**
   * Weak pointer to chunk that is used for all insert operations
   * TODO(tcies) allow user to specify what chunk to insert into
   */
  std::weak_ptr<Chunk> insert_chunk_;

};

} // namespace map_api

#endif /* NET_CR_TABLE_H_ */
