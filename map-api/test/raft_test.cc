#include <set>
#include <sys/types.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent-mapping-common/conversions.h>

#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/peer-id.h"
#include "map-api/raft-node.h"
#include "map-api/raft-chunk.h"
#include "map-api/test/testing-entrypoint.h"
#include "./consensus_fixture.h"
#include "./net_table_fixture.h"

namespace map_api {

constexpr int kWaitTimeMs = 1000;

DEFINE_uint64(raft_chunk_processes, 5u,
              "Total number of processes in RaftChunkTests");
/*
TEST_F(ConsensusFixture, RaftGetChunk) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, RaftRequestParticipation) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    REQUESTED_PARTICIPATION,
    CHUNKS_INIT,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    chunk->requestParticipation();
    IPC::barrier(REQUESTED_PARTICIPATION, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();

    IPC::barrier(REQUESTED_PARTICIPATION, kProcesses - 1);
    std::set<ChunkBase*> chunks;
    table_->getActiveChunks(&chunks);
    EXPECT_EQ(1, chunks.size());
    ChunkBase* base_chunk = *(chunks.begin());
    EXPECT_TRUE(chunk_id == base_chunk->id());
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, LeaderElection) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    LEADER_IP_SENT,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    chunk->raft_node_.giveUpLeadership();

    // Wait for a new leader
    while (!chunk->raft_node_.getLeader().isValid()) {
      VLOG(1) << "Waiting for a new leader ... ";
      usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    }
    IPC::push(chunk->raft_node_.getLeader());
    IPC::barrier(LEADER_IP_SENT, kProcesses - 1);

    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    IPC::barrier(LEADER_IP_SENT, kProcesses - 1);
    PeerId leader = IPC::pop<PeerId>();
    EXPECT_EQ(leader.ipPort(), chunk->raft_node_.getLeader().ipPort());
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, UnannouncedLeave) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    ONE_LEFT,
    DIE
  };
  enum Peers {
    LEADER,
    LEAVING_PEER
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == LEADER) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(ONE_LEFT, kProcesses - 1);
    EXPECT_EQ(kProcesses - 2, chunk->peerSize());
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());

    // One Peer leaves unannounced.
    if (getSubprocessId() == LEAVING_PEER) {
      quitRaftUnannounced(chunk);
    }
    usleep(5 * kWaitTimeMs * kMillisecondsToMicroseconds);
    IPC::barrier(ONE_LEFT, kProcesses - 1);
    EXPECT_EQ(kProcesses - 2, chunk->peerSize());
    IPC::barrier(DIE, kProcesses - 1);
  }
}

DEFINE_uint64(num_appends, 50u, "Total number entries to append");

TEST_F(ConsensusFixture, AppendLogEntries) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    START_APPEND,
    END_APPEND,
    STOP_RAFT,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(START_APPEND, kProcesses - 1);
    for (uint i = 0; i < FLAGS_num_appends; ++i) {
      leaderAppendBlankLogEntry(chunk);
    }
    leaderWaitUntilAllCommitted(chunk);
    IPC::push(PeerId::self());
    IPC::barrier(END_APPEND, kProcesses - 1);
    EXPECT_EQ(FLAGS_num_appends, getLatestEntrySerialId(chunk, PeerId::self()));

    IPC::barrier(STOP_RAFT, kProcesses - 1);
    NetTableManager::instance().forceStopAllRaftChunks();

    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    IPC::barrier(START_APPEND, kProcesses - 1);
    IPC::barrier(END_APPEND, kProcesses - 1);
    PeerId leader_id = IPC::pop<PeerId>();
    usleep(2*kWaitTimeMs*kMillisecondsToMicroseconds);
    EXPECT_EQ(FLAGS_num_appends, getLatestEntrySerialId(chunk, leader_id));

    IPC::barrier(STOP_RAFT, kProcesses - 1);
    NetTableManager::instance().forceStopAllRaftChunks();

    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, AppendLogEntriesWithPeerLeave) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    START_APPEND,
    END_APPEND,
    DIE
  };
  enum Peers {
    LEADER,
    LEAVING_PEER,
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == LEADER) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());
    IPC::barrier(START_APPEND, kProcesses - 1);

    // Append entries and wait until the are committed.
    for (uint i = 0; i < FLAGS_num_appends; ++i) {
      leaderAppendBlankLogEntry(chunk);
    }
    leaderWaitUntilAllCommitted(chunk);
    IPC::push(PeerId::self());
    IPC::barrier(END_APPEND, kProcesses - 1);
    EXPECT_EQ(FLAGS_num_appends, getLatestEntrySerialId(chunk, PeerId::self()));
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    IPC::barrier(START_APPEND, kProcesses - 1);

    // One peer leaves unannounced.
    if (getSubprocessId() == LEAVING_PEER) {
      quitRaftUnannounced(chunk);
    }
    IPC::barrier(END_APPEND, kProcesses - 1);
    PeerId leader_id = IPC::pop<PeerId>();
    if (getSubprocessId() != LEAVING_PEER) {
      usleep(2*kWaitTimeMs*kMillisecondsToMicroseconds);
      EXPECT_EQ(FLAGS_num_appends, getLatestEntrySerialId(chunk, leader_id));
    }
    IPC::barrier(DIE, kProcesses - 1);
  }
}

DEFINE_uint64(lock_request_depth, 5u,
              "Total number of processes in RaftChunkTests");

TEST_F(ConsensusFixture, RaftDistributedChunkLock) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    LEADER_LOCK,
    LEADER_INCREASE_LOCK_DEPTH,
    FOLLOWER_ATTEMPT_LEADER_RELEASE_LOCK,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);

    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::push(PeerId::self());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());

    chunk->writeLock();
    IPC::barrier(LEADER_LOCK, kProcesses - 1);

    EXPECT_TRUE(getLockHolder(chunk) == PeerId::self());
    for (uint i = 0; i < FLAGS_lock_request_depth; ++i) {
      chunk->writeLock();
    }
    IPC::barrier(LEADER_INCREASE_LOCK_DEPTH, kProcesses - 1);

    EXPECT_EQ(chunk->chunk_write_lock_depth_, FLAGS_lock_request_depth);

    for (uint i = 0; i < FLAGS_lock_request_depth + 1; ++i) {
      chunk->unlock();
    }
    EXPECT_TRUE(getLockHolder(chunk).ipPort() != PeerId::self().ipPort());
    IPC::barrier(FOLLOWER_ATTEMPT_LEADER_RELEASE_LOCK, kProcesses - 1);

    EXPECT_FALSE(getLockHolder(chunk).isValid());
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);

    common::Id chunk_id = IPC::pop<common::Id>();
    PeerId leader_id = IPC::pop<PeerId>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);

    IPC::barrier(LEADER_LOCK, kProcesses - 1);

    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    EXPECT_TRUE(getLockHolder(chunk) == leader_id);
    IPC::barrier(LEADER_INCREASE_LOCK_DEPTH, kProcesses - 1);

    EXPECT_TRUE(getLockHolder(chunk) == leader_id);
    chunk->writeLock();
    EXPECT_TRUE(getLockHolder(chunk) == PeerId::self());
    chunk->unlock();
    IPC::barrier(FOLLOWER_ATTEMPT_LEADER_RELEASE_LOCK, kProcesses - 1);

    EXPECT_FALSE(getLockHolder(chunk).isValid());
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(NetTableFixture, TransactionAbortOnPeerDisconnect) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Processes {
    ROOT,
    LEAVING_PEER,
    OTHERS
  };
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    INSERT_STARTED,
    PEER_DISCONNECT,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == ROOT) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());

    IPC::barrier(INSERT_STARTED, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    common::Id insert_id = IPC::pop<common::Id>();
    PeerId peer = IPC::pop<PeerId>();
    EXPECT_TRUE(peer == chunk->getLockHolder());

    IPC::barrier(PEER_DISCONNECT, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    EXPECT_EQ(kProcesses - 2, chunk->peerSize());
    EXPECT_FALSE(chunk->constData()->getById(insert_id, LogicalTime::sample()));
    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);

    if (getSubprocessId() == LEAVING_PEER) {
      ChunkTransaction transaction(chunk, table_);
      common::Id insert_id = insert(42, &transaction);
      IPC::push(insert_id);
      IPC::push(PeerId::self());

      // Partial commit procedure.
      chunk->writeLock();
      EXPECT_TRUE(transaction.check());
      transaction.checkedCommit(LogicalTime::sample());
      IPC::barrier(INSERT_STARTED, kProcesses - 1);

      IPC::barrier(PEER_DISCONNECT, kProcesses - 1);
      chunk->forceStopRaft();
      IPC::barrier(DIE, kProcesses - 1);
    } else {
      IPC::barrier(INSERT_STARTED, kProcesses - 1);
      usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
      common::Id insert_id = IPC::pop<common::Id>();
      PeerId peer = IPC::pop<PeerId>();
      EXPECT_TRUE(peer == chunk->getLockHolder());

      IPC::barrier(PEER_DISCONNECT, kProcesses - 1);
      usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
      EXPECT_EQ(kProcesses - 2, chunk->peerSize());
      EXPECT_FALSE(
          chunk->constData()->getById(insert_id, LogicalTime::sample()));
      IPC::barrier(DIE, kProcesses - 1);
    }
  }
}*/

TEST_F(ConsensusFixture, MultiChunkTransaction) {
  const uint64_t kProcesses = FLAGS_raft_chunk_processes;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    CHUNKS_INIT,
    INITIAL_INSERT,
    FORCE_STOP_RAFT,
    DIE
  };
  enum Fields {
    kFieldName
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    VLOG(1) << "Creating a new chunk.";
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    ChunkBase* base_chunk = table_->newChunk();
    VLOG(1) << "Created a new chunk " << base_chunk->id();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);
    VLOG(1) << "Chunks initialized on all peers";
    EXPECT_EQ(kProcesses - 1, chunk->peerSize());

    common::Id insert_id;
    generateId(&insert_id);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->setId(insert_id);
    to_insert->set(kFieldName, 42);
    Transaction initial_insert;
    initial_insert.insert(table_, chunk, to_insert);
    initial_insert.multiChunkCommit();
    IPC::push(insert_id);
    IPC::barrier(INITIAL_INSERT, kProcesses - 1);

    IPC::barrier(FORCE_STOP_RAFT, kProcesses - 1);
    NetTableManager::instance().forceStopAllRaftChunks();
    IPC::barrier(DIE, kProcesses - 1);

  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    ChunkBase* base_chunk = table_->getChunk(chunk_id);
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::barrier(CHUNKS_INIT, kProcesses - 1);

    IPC::barrier(INITIAL_INSERT, kProcesses - 1);
    common::Id insert_id = IPC::pop<common::Id>();
    usleep(5 * kWaitTimeMs * kMillisecondsToMicroseconds);
    Transaction reader;
    EXPECT_TRUE(
        reader.getById(insert_id, table_, chunk)->verifyEqual(kFieldName, 42));

    IPC::barrier(FORCE_STOP_RAFT, kProcesses - 1);
    NetTableManager::instance().forceStopAllRaftChunks();
    IPC::barrier(DIE, kProcesses - 1);
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
