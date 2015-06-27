#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/transaction.h"
#include "./net_table_fixture.h"

namespace map_api {

class TransactionTest : public NetTableFixture {
 protected:
  virtual void SetUp() {
    NetTableFixture::SetUp();
    chunk_id_.fromHexString("00000000000000000000000000000042");
    if (getSubprocessId() == 0) {
      chunk_ = table_->newChunk(chunk_id_);
    }
  }
};

TEST_F(TransactionTest, TransactionMerge) {
  enum Processes {
    ROOT,
    A
  };
  enum Barriers {
    INIT,
    CHECKOUT,
    A_COMMITTED
  };
  common::Id a_id, b_id;
  if (getSubprocessId() == ROOT) {
    a_id = insert(42, chunk_);
    b_id = insert(21, chunk_);
    launchSubprocess(A);

    IPC::barrier(INIT, 1);
    IPC::push(a_id);
    Transaction transaction;
    IPC::barrier(CHECKOUT, 1);
    increment(table_, a_id, chunk_, &transaction);
    increment(table_, b_id, chunk_, &transaction);
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
    chunk_ = table_->getChunk(chunk_id_);
    Transaction transaction;
    IPC::barrier(CHECKOUT, 1);
    a_id = IPC::pop<common::Id>();
    increment(table_, a_id, chunk_, &transaction);
    ASSERT_TRUE(transaction.commit());
    IPC::barrier(A_COMMITTED, 1);
  }
}

TEST_F(TransactionTest, MultiCommit) {
  Transaction transaction;
  common::Id inserted_id_1, inserted_id_2;

  insert(1, &inserted_id_1, &transaction);
  EXPECT_TRUE(transaction.commit());
  EXPECT_EQ(1u, count());

  insert(2, &inserted_id_2, &transaction);
  update(3, inserted_id_1, &transaction);
  EXPECT_TRUE(transaction.commit());
  EXPECT_EQ(2u, count());

  Transaction perturber;
  update(4, inserted_id_2, &perturber);
  EXPECT_TRUE(perturber.commit());

  transaction.remove(inserted_id_1, table_);
  EXPECT_TRUE(transaction.commit());
  EXPECT_EQ(1u, count());

  update(5, inserted_id_2, &transaction);
  EXPECT_FALSE(transaction.commit());
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
