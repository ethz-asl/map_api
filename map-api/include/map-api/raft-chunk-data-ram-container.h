#ifndef MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
#define MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_

#include <vector>

#include "./raft.pb.h"
#include "map-api/chunk-data-container-base.h"

namespace map_api {
class ReaderWriterMutex;

// TODO(aqurai): When implementing STXXL container, split into a base class,
// and derived classes for RAM and STXXL containers
class RaftChunkDataRamContainer : public ChunkDataContainerBase {  
 public:
  virtual ~RaftChunkDataRamContainer();

  // ========
  // RAFT-LOG
  // ========

  // TODO(aqurai): Make this class private. The prolem is iterator and
  // const_iterator are not defined within RaftNode.
  class RaftLog : public std::vector<std::shared_ptr<proto::RaftLogEntry>> {
   public:
    virtual ~RaftLog() {}
    iterator getLogIteratorByIndex(uint64_t index);
    const_iterator getConstLogIteratorByIndex(uint64_t index) const;
    uint64_t eraseAfter(iterator it);
    inline uint64_t lastLogIndex() const { return back()->index(); }
    inline uint64_t lastLogTerm() const { return back()->term(); }
    inline common::ReaderWriterMutex* mutex() const { return &log_mutex_; }

    // Yet to be implemented:
    // void commitNextEnty() {}
    // void commitUntilIndex(uint64_t index) {}
    // uint64_t commit_index();
    // std::unordered_map<int, std::function<void(const proto::RaftLogEntry*)>&>
    // commit_actions;

   private:
    mutable common::ReaderWriterMutex log_mutex_;
    mutable std::mutex commit_mutex_;
    uint64_t commit_index_;
  };

  class LogReadAccess {
  public:
    explicit LogReadAccess(const RaftChunkDataRamContainer*);
    ~LogReadAccess();
    const RaftLog* operator->() const;
    void unlockAndDisable();
    LogReadAccess() = delete;
    LogReadAccess(const LogReadAccess&) = delete;
    LogReadAccess& operator=(const LogReadAccess&) = delete;
    LogReadAccess(LogReadAccess&&) = delete;
    LogReadAccess& operator=(LogReadAccess&&) = delete;
  private:
    const RaftLog* read_log_;
    bool is_enabled_;
  };

  class LogWriteAccess {
   public:
    explicit LogWriteAccess(RaftChunkDataRamContainer*);
    ~LogWriteAccess();
    RaftLog* operator->() const;
    void unlockAndDisable();
    LogWriteAccess() = delete;
    LogWriteAccess(const LogWriteAccess&) = delete;
    LogWriteAccess& operator=(const LogWriteAccess&) = delete;
    LogWriteAccess(LogWriteAccess&&) = delete;
    LogWriteAccess& operator=(LogWriteAccess&&) = delete;

   private:
    RaftLog* write_log_;
    bool is_enabled_;
  };

 private:
  friend class LogReadAccess;
  // friend class RaftNode; // TODO(aqurai): Remove friendship?

  // READ OPERATIONS INHERITED FROM PARENT
  virtual bool initImpl();
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const common::Id& id, const LogicalTime& time) const;
  // If key is -1, this should return all the data in the table.
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<common::Id>* ids) const;
  // If key is -1, this should return all the data in the table.
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const;
  // =================
  // HISTORY CONTAINER
  // =================
  // TODO(aqurai): Implement history container here.
  // This is an incomplete implementation.
  class History : public std::list<std::shared_ptr<const Revision>> {
   public:
    virtual ~History() {}
    inline const_iterator latestAt(const LogicalTime& time) const {
      for (const_iterator it = cbegin(); it != cend(); ++it) {
        if ((*it)->getUpdateTime() <= time) return it;
      }
      return cend();
    }
  };
  typedef std::unordered_map<common::Id, History> HistoryMap;
  HistoryMap data_;

 private:
  RaftLog log_;
};

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
