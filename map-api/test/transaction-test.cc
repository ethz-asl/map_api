#include <map-api/transaction.h>

#include <multiagent-mapping-common/test/testing-entrypoint.h>

#include <map-api/ipc.h>
#include "./net_table_fixture.h"

namespace map_api {

TEST_P(NetTableFixture, TransactionMerge) {
  if (!GetParam()) {
    return;
  }
  enum Processes {
    ROOT,
    A
  };
  enum Barriers {
    INIT,
    CHECKOUT,
    A_COMMITTED
  };
  Chunk* chunk;
  Id chunk_id, a_id, b_id;
  chunk_id.fromHexString("00000000000000000000000000000042");
  if (getSubprocessId() == ROOT) {
    chunk = table_->newChunk(chunk_id);
    a_id = insert(42, chunk);
    b_id = insert(21, chunk);
    launchSubprocess(A);

    IPC::barrier(INIT, 1);
    IPC::push(a_id);
    Transaction transaction;
    IPC::barrier(CHECKOUT, 1);
    increment(table_, a_id, chunk, &transaction);
    increment(table_, b_id, chunk, &transaction);
    IPC::barrier(A_COMMITTED, 1);
    EXPECT_FALSE(transaction.commit());
    std::shared_ptr<Transaction> merge_transaction(new Transaction);
    Transaction::ConflictMap conflicts;
    transaction.merge(merge_transaction, &conflicts);
    EXPECT_EQ(1u, merge_transaction->numChangedItems());
    EXPECT_EQ(1u, conflicts.size());
    EXPECT_EQ(1u, conflicts[table_].size());
    EXPECT_TRUE(conflicts[table_].begin()->ours->verifyEqual(kFieldName, 43));
    EXPECT_TRUE(conflicts[table_].begin()->theirs->verifyEqual(kFieldName, 43));
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    chunk = table_->getChunk(chunk_id);
    Transaction transaction;
    IPC::barrier(CHECKOUT, 1);
    a_id = IPC::pop<Id>();
    increment(table_, a_id, chunk, &transaction);
    ASSERT_TRUE(transaction.commit());
    IPC::barrier(A_COMMITTED, 1);
  }
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
