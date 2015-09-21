#ifndef MAP_API_NET_TABLE_FIXTURE_H_
#define MAP_API_NET_TABLE_FIXTURE_H_

#include <string>

#include <gtest/gtest.h>

#include <map-api/net-table.h>
#include <map-api/net-table-transaction.h>
#include <map-api/transaction.h>
#include "./map_api_fixture.h"

namespace map_api {

class NetTableFixture : public MapApiFixture {
 public:
  enum Fields {
    kFieldName
  };

  virtual ~NetTableFixture() {}

 protected:
  virtual void SetUp();

  size_t count();

  void increment(const common::Id& id, ChunkBase* chunk,
                 NetTableTransaction* transaction);
  void increment(NetTable* table, const common::Id& id, ChunkBase* chunk,
                 Transaction* transaction);

  common::Id insert(int n, ChunkBase* chunk);
  common::Id insert(int n, ChunkTransaction* transaction);
  void insert(int n, common::Id* id, Transaction* transaction);

  void update(int n, const common::Id& id, Transaction* transaction);

  static const std::string kTableName;
  NetTable* table_;
  ChunkBase* chunk_;               // generic chunk pointer for custom use
  common::Id chunk_id_, item_id_;  // equally generic
};

}  // namespace map_api

#include "./net_table_fixture_inl.h"

#endif  // MAP_API_NET_TABLE_FIXTURE_H_
