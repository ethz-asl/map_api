#ifndef MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
#define MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_

#include <list>
#include <string>
#include <vector>

#include "./raft.pb.h"
#include "map-api/chunk-data-container-base.h"

namespace map_api {
class ReaderWriterMutex;

// TODO(aqurai): When implementing STXXL container, split into a base class,
// and derived classes for RAM and STXXL containers
class RaftChunkDataRamContainer : public ChunkDataContainerBase {
 public:
  friend class RaftNode;
  friend class RaftChunk;
  virtual ~RaftChunkDataRamContainer();

 private:
  // friend class LogReadAccess;

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

  // CHECK/PREPARE INSERT AND UPDATE
  bool checkAndPrepareInsert(const LogicalTime& time,
                             const std::shared_ptr<Revision>& query);
  bool checkAndPrepareUpdate(const LogicalTime& time,
                             const std::shared_ptr<Revision>& query);
  bool checkAndPrepareBulkInsert(const LogicalTime& time,
                                 const MutableRevisionMap& query);

  // INSERT
  bool checkAndPatch(const std::shared_ptr<Revision>& query);

  bool patch(const Revision::ConstPtr& query);

  // =================
  // HISTORY CONTAINER
  // =================
  class History : public std::list<std::shared_ptr<const Revision>> {
   public:
    virtual ~History() {}
    inline const_iterator latestAt(const LogicalTime& time) const;
  };
  typedef std::unordered_map<common::Id, History> HistoryMap;
  HistoryMap data_;

  inline void forEachItemFoundAtTime(
      int key, const Revision& value_holder,
      const LogicalTime& time,
      const std::function<void(
          const common::Id& id,
          const std::shared_ptr<const Revision>& item)>& action) const;
  inline void forChunkItemsAtTime(
      const common::Id& chunk_id, const LogicalTime& time,
      const std::function<void(
          const common::Id& id,
          const std::shared_ptr<const Revision>& item)>& action) const;
  inline void trimToTime(const LogicalTime& time, HistoryMap* subject) const;

  // OTHER READ OPERATIONS
  void chunkHistory(const common::Id& chunk_id, const LogicalTime& time,
                    HistoryMap* dest) const;

  // ========
  // RAFT-LOG
  // ========
  class RaftLog : public std::vector<std::shared_ptr<proto::RaftLogEntry>> {
   public:
    RaftLog();
    virtual ~RaftLog() {}
    iterator getLogIteratorByIndex(uint64_t index);
    const_iterator getConstLogIteratorByIndex(uint64_t index) const;
    proto::RaftLogEntry* copyWithoutRevision(const_iterator it) const;
    uint64_t getEntryIndex(const PeerId& peer, uint64_t serial_id) const;
    uint64_t getPeerLatestSerialId(const PeerId& peer) const;
    uint64_t eraseAfter(iterator it);
    inline uint64_t lastLogIndex() const { return back()->index(); }
    inline uint64_t lastLogTerm() const { return back()->term(); }
    inline uint64_t commitIndex() const { return commit_index_; }
    inline common::ReaderWriterMutex* mutex() const { return &log_mutex_; }

    void appendLogEntry(const std::shared_ptr<proto::RaftLogEntry>& entry);
    inline void setCommitIndex(uint64_t value) { commit_index_ = value; }
    uint64_t setEntryCommitted(iterator it);

   private:
    friend class RaftChunkDataRamContainer;
    using std::vector<std::shared_ptr<proto::RaftLogEntry>>::push_back;
    std::unordered_map<std::string, uint64_t> serial_id_map_;
    mutable common::ReaderWriterMutex log_mutex_;
    uint64_t commit_index_;
  };
  RaftLog log_;
  inline uint64_t logCommitIndex() const;
  inline uint64_t lastLogTerm() const;

  class LogReadAccess {
   public:
    explicit LogReadAccess(const RaftChunkDataRamContainer* container);
    ~LogReadAccess();
    const RaftLog* operator->() const;
    void unlockAndDisable();
    LogReadAccess() = delete;
    LogReadAccess(const LogReadAccess&) = delete;
    LogReadAccess& operator=(const LogReadAccess&) = delete;
  private:
    const RaftLog* read_log_;
    bool is_enabled_;
  };

  class LogWriteAccess {
   public:
    explicit LogWriteAccess(RaftChunkDataRamContainer* container);
    ~LogWriteAccess();
    RaftLog* operator->() const;
    void unlockAndDisable();
    LogWriteAccess() = delete;
    LogWriteAccess(const LogWriteAccess&) = delete;
    LogWriteAccess& operator=(const LogWriteAccess&) = delete;
   private:
    RaftLog* write_log_;
    bool is_enabled_;
  };
};

}  // namespace map_api

#include "./raft-chunk-data-ram-container-inl.h"

#endif  // MAP_API_RAFT_CHUNK_DATA_RAM_CONTAINER_H_
