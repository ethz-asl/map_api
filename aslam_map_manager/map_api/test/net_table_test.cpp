#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/ipc.h"
#include "map-api/net-table-transaction.h"

#include "net_table_test_fixture.cpp"

using namespace map_api;

/**
 * Observation: A does all commits before B does all commits. This makes
 * sense because A's operations per transaction are less complex, thus
 * faster executed, and A gets to lock first.
 */
TEST_P(NetTableTest, NetTableTransactions) {
  if (!GetParam()) {
    return;
  }
  enum Processes {ROOT, A, B};
  enum Barriers {INIT, SYNC, DIE};
  int kCycles = 10;
  if (getSubprocessId() == ROOT) {
    Chunk* ab_chunk = table_->newChunk(), *b_chunk = table_->newChunk();
    Id ab_id = insert(0, ab_chunk), b_id = insert(0, b_chunk);
    launchSubprocess(A);
    launchSubprocess(B);

    IPC::barrier(INIT, 2);
    IPC::push(ab_id.hexString());
    IPC::push(b_id.hexString());
    ab_chunk->requestParticipation();
    b_chunk->requestParticipation();

    IPC::barrier(SYNC, 2);
    IPC::barrier(DIE, 2);
    EXPECT_TRUE(table_->getById(ab_id, Time::now())->
                verify(kFieldName, 2 * kCycles));
    EXPECT_TRUE(table_->getById(b_id, Time::now())->
                verify(kFieldName, kCycles));
    std::unordered_map<Id, std::shared_ptr<Revision> > results;
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(kCycles + 2, results.size());
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    Id ab_id, chunk_id;
    std::string ab_id_string;
    IPC::pop(&ab_id_string);
    ab_id.fromHexString(ab_id_string);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(Time::now(), table_);

        std::shared_ptr<Revision> to_update = attempt.getById(ab_id);
        int transient_value;
        to_update->get(kFieldName, &transient_value);
        to_update->get(NetTable::kChunkIdField, &chunk_id);
        ++transient_value;
        to_update->set(kFieldName, transient_value);
        attempt.update(to_update);

        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        to_insert->set(CRTable::kIdField, Id::generate());
        to_insert->set(kFieldName, 42);
        attempt.insert(table_->getChunk(chunk_id), to_insert);

        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
  if (getSubprocessId() == B) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    Id ab_id, b_id;
    std::string ab_id_string, b_id_string;
    IPC::pop(&ab_id_string);
    IPC::pop(&b_id_string);
    ab_id.fromHexString(ab_id_string);
    b_id.fromHexString(b_id_string);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(Time::now(), table_);

        std::shared_ptr<Revision> to_update = attempt.getById(ab_id);
        int transient_value;
        to_update->get(kFieldName, &transient_value);
        ++transient_value;
        to_update->set(kFieldName, transient_value);
        attempt.update(to_update);

        to_update = attempt.getById(b_id);
        to_update->get(kFieldName, &transient_value);
        ++transient_value;
        to_update->set(kFieldName, transient_value);
        attempt.update(to_update);

        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
