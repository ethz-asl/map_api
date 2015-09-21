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
