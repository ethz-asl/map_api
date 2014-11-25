#ifndef MAP_API_NET_TABLE_FIXTURE_H_
#define MAP_API_NET_TABLE_FIXTURE_H_

#include <string>

#include <gtest/gtest.h>

#include <map-api/net-table.h>
#include <map-api/net-table-transaction.h>
#include <map-api/transaction.h>
#include "./map_api_fixture.h"

namespace map_api {

class NetTableFixture : public MapApiFixture,
                        public ::testing::WithParamInterface<bool> {
 public:
  enum Fields {
    kFieldName
  };

 protected:
  virtual void SetUp();

  size_t count();

  void increment(const Id& id, Chunk* chunk, NetTableTransaction* transaction);
  void increment(NetTable* table, const Id& id, Chunk* chunk,
                 Transaction* transaction);

  Id insert(int n, Chunk* chunk);
  Id insert(int n, ChunkTransaction* transaction);
  void insert(int n, Id* id, Transaction* transaction);

  void update(int n, const Id& id, Transaction* transaction);

  static const std::string kTableName;
  NetTable* table_;
  Chunk* chunk_;           // generic chunk pointer for custom use
  Id chunk_id_, item_id_;  // equally generic
};

}  // namespace map_api

#include "./net_table_fixture_inl.h"

#endif  // MAP_API_NET_TABLE_FIXTURE_H_
