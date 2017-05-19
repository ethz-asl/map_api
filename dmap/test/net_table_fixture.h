#ifndef DMAP_NET_TABLE_FIXTURE_H_
#define DMAP_NET_TABLE_FIXTURE_H_

#include <string>

#include <gtest/gtest.h>

#include <dmap/net-table.h>
#include <dmap/net-table-transaction.h>
#include <dmap/transaction.h>
#include "./dmap_fixture.h"

namespace dmap {

class NetTableFixture : public MapApiFixture {
 public:
  enum Fields {
    kFieldName
  };

  virtual ~NetTableFixture() {}

 protected:
  virtual void SetUp();

  size_t count();

  void increment(const dmap_common::Id& id, ChunkBase* chunk,
                 NetTableTransaction* transaction);
  void increment(NetTable* table, const dmap_common::Id& id, ChunkBase* chunk,
                 Transaction* transaction);

  dmap_common::Id insert(int n, ChunkBase* chunk);
  dmap_common::Id insert(int n, ChunkTransaction* transaction);
  void insert(int n, dmap_common::Id* id, Transaction* transaction);
  void insert(int n, const dmap_common::Id& id, Transaction* transaction);

  template <typename IdType>
  void update(int n, const IdType& id, Transaction* transaction);

  static const std::string kTableName;
  NetTable* table_;
  ChunkBase* chunk_;               // generic chunk pointer for custom use
  dmap_common::Id chunk_id_, item_id_;  // equally generic
};

}  // namespace dmap

#include "./net_table_fixture_inl.h"

#endif  // DMAP_NET_TABLE_FIXTURE_H_
