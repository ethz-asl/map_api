#include <cstdio>
#include <map>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <map-api/chunk-data-container-base.h>

#include "./core.pb.h"
#include <map-api/core.h>

namespace map_api {

ChunkDataContainerBase::ChunkDataContainerBase() : initialized_(false) {}

ChunkDataContainerBase::~ChunkDataContainerBase() {}

bool ChunkDataContainerBase::init(std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(descriptor);
  CHECK(descriptor->has_name());
  descriptor_ = descriptor;
  CHECK(initImpl());
  initialized_ = true;
  return true;
}

bool ChunkDataContainerBase::isInitialized() const { return initialized_; }

const std::string& ChunkDataContainerBase::name() const {
  return descriptor_->name();
}

std::shared_ptr<Revision> ChunkDataContainerBase::getTemplate() const {
  CHECK(isInitialized()) << "Can't get template of non-initialized table";
  return descriptor_->getTemplate();
}

bool ChunkDataContainerBase::insert(const LogicalTime& time,
                                    const std::shared_ptr<Revision>& query) {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(query.get() != nullptr);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference))
  << "Bad structure of insert revision";
  CHECK(query->getId<common::Id>().isValid())
  << "Attempted to insert element with invalid ID";
  query->setInsertTime(time);
  query->setUpdateTime(time);
  return insertImpl(query);
}

bool ChunkDataContainerBase::bulkInsert(const LogicalTime& time,
                                        const MutableRevisionMap& query) {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  common::Id id;
  for (const typename MutableRevisionMap::value_type& id_revision : query) {
    CHECK_NOTNULL(id_revision.second.get());
    CHECK(id_revision.second->structureMatch(*reference))
    << "Bad structure of insert revision";
    id = id_revision.second->getId<common::Id>();
    CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
    CHECK(id == id_revision.first) << "ID in RevisionMap doesn't match";
    id_revision.second->setInsertTime(time);
    id_revision.second->setUpdateTime(time);
  }
  return bulkInsertImpl(query);
}

bool ChunkDataContainerBase::patch(
    const std::shared_ptr<const Revision>& query) {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(query != nullptr);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference)) << "Bad structure of patch revision";
  CHECK(query->getId<common::Id>().isValid())
  << "Attempted to insert element with invalid ID";
  return patchImpl(query);
}

void ChunkDataContainerBase::findByRevision(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time,
                                            ConstRevisionMap* dest) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  // whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK(time < LogicalTime::sample())
  << "Seeing the future is yet to be implemented ;)";
  findByRevisionImpl(key, valueHolder, time, dest);
}

int ChunkDataContainerBase::numAvailableIds(const LogicalTime& time) const {
  return count(-1, 0, time);
}

int ChunkDataContainerBase::countByRevision(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to count items in non-initialized table";
  // Whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here.
  CHECK(time < LogicalTime::sample())
  << "Seeing the future is yet to be implemented ;)";
  return countByRevisionImpl(key, valueHolder, time);
}

void ChunkDataContainerBase::dump(const LogicalTime& time,
                                  ConstRevisionMap* dest) const {
  CHECK_NOTNULL(dest);
  std::shared_ptr<Revision> valueHolder = getTemplate();
  CHECK(valueHolder != nullptr);
  findByRevision(-1, *valueHolder, time, dest);
}

ChunkDataContainerBase::History::~History() {}

void ChunkDataContainerBase::findHistoryByRevision(int key,
                                                   const Revision& valueHolder,
                                                   const LogicalTime& time,
                                                   HistoryMap* dest) const {
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK(time < LogicalTime::sample());
  return findHistoryByRevisionImpl(key, valueHolder, time, dest);
}

void ChunkDataContainerBase::update(const LogicalTime& time,
                                    const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  // TODO(tcies) const template, cow template?
  CHECK(query->structureMatch(*reference))
  << "Bad structure of update revision";
  CHECK(query->getId<common::Id>().isValid())
  << "Attempted to update element with invalid ID";
  LogicalTime update_time = time;
  query->setUpdateTime(update_time);
  CHECK(insertUpdatedImpl(query));
}

void ChunkDataContainerBase::remove(const LogicalTime& time,
                                    const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK(isInitialized());
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference));
  CHECK_NE(query->getId<common::Id>(), common::Id());
  LogicalTime update_time = time;
  query->setUpdateTime(update_time);
  query->setRemoved();
  CHECK(insertUpdatedImpl(query));
}

void ChunkDataContainerBase::clear() {
  std::lock_guard<std::mutex> lock(access_mutex_);
  clearImpl();
}

std::ostream& operator<<(std::ostream& stream,
                         const ChunkDataContainerBase::ItemDebugInfo& info) {
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

bool ChunkDataContainerBase::getLatestUpdateTime(const common::Id& id,
                                                 LogicalTime* time) {
  CHECK_NE(common::Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<const Revision> row = getById(id, LogicalTime::sample());
  ItemDebugInfo itemInfo(name(), id);
  if (!row) {
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  *time = row->getUpdateTime();
  return true;
}

} /* namespace map_api */
