#ifndef DMAP_NET_TABLE_FIXTURE_H_
#define DMAP_NET_TABLE_FIXTURE_H_

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

  void increment(const map_api_common::Id& id, ChunkBase* chunk,
                 NetTableTransaction* transaction);
  void increment(NetTable* table, const map_api_common::Id& id, ChunkBase* chunk,
                 Transaction* transaction);

  map_api_common::Id insert(int n, ChunkBase* chunk);
  map_api_common::Id insert(int n, ChunkTransaction* transaction);
  void insert(int n, map_api_common::Id* id, Transaction* transaction);
  void insert(int n, const map_api_common::Id& id, Transaction* transaction);

  template <typename IdType>
  void update(int n, const IdType& id, Transaction* transaction);

  static const std::string kTableName;
  NetTable* table_;
  ChunkBase* chunk_;               // generic chunk pointer for custom use
  map_api_common::Id chunk_id_, item_id_;  // equally generic
};

}  // namespace map_api

#include "./net_table_fixture_inl.h"

#endif  // DMAP_NET_TABLE_FIXTURE_H_
