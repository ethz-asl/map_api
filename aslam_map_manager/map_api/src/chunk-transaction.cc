#include "map-api/chunk-transaction.h"

namespace map_api {

void ChunkTransaction::insert(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  Id id;
  revision->get(CRTable::kIdField, &id);
  CHECK(insertions_.insert(std::make_pair(id, revision)).second);
}

void ChunkTransaction::update(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  CHECK(cache_->type() == CRTable::Type::CRU);
  Id id;
  revision->get(CRTable::kIdField, &id);
  CHECK(updates_.insert(std::make_pair(id, revision)).second);
}

std::shared_ptr<Revision> ChunkTransaction::getById(const Id& id) {
  // TODO(tcies) uncommitted!
  return cache_->getById(id, begin_time_);
}

ChunkTransaction::ChunkTransaction(const Time& begin_time, CRTable* cache)
: begin_time_(begin_time), cache_(CHECK_NOTNULL(cache)) {
  CHECK(begin_time <= Time::now());
  insertions_.clear();
  updates_.clear();
  structure_reference_ = cache_->getTemplate();
}

} /* namespace map_api */
