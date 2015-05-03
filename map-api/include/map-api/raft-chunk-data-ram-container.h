#ifndef MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
#define MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_

#include <list>
#include <vector>

#include "map-api/chunk-data-container-base.h"
#include "./raft.pb.h"

namespace map_api {
class ReaderWriterMutex;

// TODO(aqurai): When implementing STXXL container, split into a base class,
// and derived classes for RAM and STXXL containers
class RaftChunkDataRamContainer : public ChunkDataContainerBase {
  
  virtual ~RaftChunkDataRamContainer();

 public:
  // Check before commit. There are no checked commits here.
  //

 private:
  friend class LogReadAccess;
  // remove friendship.
  friend class RaftNode;

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
  // TODO(aqurai): We are storing the same shared_ptr twice: in raft log and
  // here. Either remove from raft log (which means log is modified on commit)
  // or store reference to raft log (indices) here, which requires revisions
  // to be stored in log, and the log will need a completely different impl for
  // STXXL container.
  class History : public std::list<std::shared_ptr<const Revision>> {
   public:
    virtual ~History() {}
    inline const_iterator latestAt(const LogicalTime& time) const;
  };
  typedef std::unordered_map<common::Id, History> HistoryMap;
  HistoryMap data_;

  // Read access
  // Access to revisions etc

  // Write access
  // only from raft log. insert and update
  // ========
  // RAFT-LOG
  // ========

  // TODO(aqurai): const proto::RaftRevision?
  // template <>
  class RaftLog : public std::vector<std::shared_ptr<proto::RaftLogEntry>> {
   public:
    virtual ~RaftLog() {}
    iterator getLogIteratorByIndex(uint64_t index);
    const_iterator getConstLogIteratorByIndex(uint64_t index);
    uint64_t eraseAfter(iterator it);
    inline uint64_t lastLogIndex() const { return back()->index(); }
    inline uint64_t lastLogTerm() const { return back()->term(); }
    inline common::ReaderWriterMutex* mutex() const { return &log_mutex_; }

    // void commitNextEnty() {}
    // void commitUntilIndex(uint64_t index) {}
    // uint64_t commit_index();
    // std::unordered_map<int, std::function<void(const proto::RaftRevision*)>&>
    // commit_actions;

   private:
    mutable common::ReaderWriterMutex log_mutex_;
    mutable std::mutex commit_mutex_;
    uint64_t commit_index_;
  };
  RaftLog log_;
  
  class LogReadAccess {
  public:
    explicit LogReadAccess(const RaftChunkDataRamContainer*);
    ~LogReadAccess();
    const RaftLog* operator->() const;
    LogReadAccess() = delete;
    LogReadAccess(const LogReadAccess&) = delete;
    LogReadAccess& operator=(const LogReadAccess&) = delete;
    LogReadAccess(LogReadAccess&&) = delete;
    LogReadAccess& operator=(LogReadAccess&&) = delete;
  private:
    const RaftLog* read_log_;
  };
  
  class LogWriteAccess {
  public:
    explicit LogWriteAccess(RaftChunkDataRamContainer*);
    ~LogWriteAccess();
    RaftLog* operator->() const;
    LogWriteAccess() = delete;
    LogWriteAccess(const LogWriteAccess&) = delete;
    LogWriteAccess& operator=(const LogWriteAccess&) = delete;
    LogWriteAccess(LogWriteAccess&&) = delete;
    LogWriteAccess& operator=(LogWriteAccess&&) = delete;
  private:
    RaftLog* write_log_;
  };

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder, const LogicalTime& time,
      const std::function<void(const common::Id& id, const Revision& item)>&
          action) const;
};

}  // namespace map_api

#include "map-api/raft-chunk-data-ram-container-inl.h"

#endif  // MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
