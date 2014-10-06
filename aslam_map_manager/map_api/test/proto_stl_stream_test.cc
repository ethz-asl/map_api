#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/proto-stl-stream.h"

namespace map_api {
template<int Size>
struct SizeHolder {
  enum {
    value = Size
  };
};

template<typename SizeHolder_T>
class MemoryBlockPoolTest : public ::testing::TestWithParam<int> {
  enum {
    kBlockSize = SizeHolder_T::value
  };
 protected:
  virtual void SetUp() {
    block_size_ = kBlockSize;
  }
  int block_size_;
  MemoryBlockPool<kBlockSize, std::vector> pool_;
};

TYPED_TEST_CASE_P(MemoryBlockPoolTest);

TYPED_TEST_P(MemoryBlockPoolTest, ReserveWorks) {
  this->pool_.Reserve(10);
}

TYPED_TEST_P(MemoryBlockPoolTest, FullBlockSizeReturnWorks) {
  unsigned char* data = nullptr;
  int size = 0;
  EXPECT_EQ(0, this->pool_.BlockIndex());
  EXPECT_EQ(this->block_size_, this->pool_.PositionInCurrentBlock());

  this->pool_.Next(&data, &size);

  EXPECT_NE(data, nullptr);
  EXPECT_EQ(this->block_size_, size);
  EXPECT_EQ(0, this->pool_.BlockIndex());
  EXPECT_EQ(this->block_size_, this->pool_.PositionInCurrentBlock());
}

TYPED_TEST_P(MemoryBlockPoolTest, BackupWorks) {
  unsigned char* data1 = nullptr;
  int size = 0;
  this->pool_.Next(&data1, &size);

  EXPECT_NE(data1, nullptr);
  EXPECT_EQ(this->block_size_, size);

  this->pool_.BackUp(2);
  EXPECT_EQ(this->block_size_ - 2, this->pool_.PositionInCurrentBlock());

  unsigned char* data2 = nullptr;
  this->pool_.Next(&data2, &size);

  EXPECT_EQ(data1 + this->block_size_ - 2, data2);
  EXPECT_EQ(2, size);
  EXPECT_EQ(this->block_size_, this->pool_.PositionInCurrentBlock());
}

TYPED_TEST_P(MemoryBlockPoolTest, BackupOverBlockBoundsWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  unsigned char* data1 = nullptr;
  int size = 0;
  this->pool_.Next(&data1, &size);
  EXPECT_EQ(0, this->pool_.BlockIndex());
  unsigned char* data2 = nullptr;
  this->pool_.Next(&data2, &size);
  EXPECT_EQ(1, this->pool_.BlockIndex());

  this->pool_.BackUp(this->block_size_ + 2);
  EXPECT_EQ(this->block_size_ - 2, this->pool_.PositionInCurrentBlock());
  EXPECT_EQ(0, this->pool_.BlockIndex());

  unsigned char* data3 = nullptr;
  this->pool_.Next(&data3, &size);

  EXPECT_EQ(0, this->pool_.BlockIndex());
  EXPECT_EQ(this->block_size_, this->pool_.PositionInCurrentBlock());
  EXPECT_EQ(data1 + this->block_size_ - 2, data3);
  EXPECT_EQ(2, size);
}

TYPED_TEST_P(MemoryBlockPoolTest, RetrieveDataBlockWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  int size = 0;
  unsigned char* data1 = nullptr;
  this->pool_.Next(&data1, &size);
  unsigned char* data2 = nullptr;
  this->pool_.Next(&data2, &size);

  const unsigned char* data_get = nullptr;
  int size_get = 0;
  this->pool_.RetrieveDataBlock(0, 0, &data_get, &size_get);
  EXPECT_EQ(this->block_size_, size_get);
  EXPECT_EQ(data1, data_get);

  this->pool_.RetrieveDataBlock(0, 1, &data_get, &size_get);
  EXPECT_EQ(this->block_size_ - 1, size_get);
  EXPECT_EQ(data1 + 1, data_get);

  this->pool_.RetrieveDataBlock(1, 0, &data_get, &size_get);
  EXPECT_EQ(this->block_size_, size_get);
  EXPECT_EQ(data2, data_get);

  this->pool_.RetrieveDataBlock(1, 2, &data_get, &size_get);
  EXPECT_EQ(this->block_size_ - 2, size_get);
  EXPECT_EQ(data2 + 2, data_get);
}

TYPED_TEST_P(MemoryBlockPoolTest, SizeWorks) {
  EXPECT_EQ(this->pool_.Size(), 0);
  unsigned char* data = nullptr;
  int size = 0;
  this->pool_.Next(&data, &size);
  EXPECT_EQ(this->pool_.Size(), 1);
  this->pool_.Next(&data, &size);
  EXPECT_EQ(this->pool_.Size(), 2);
}

TYPED_TEST_P(MemoryBlockPoolTest, IsIndexInBoundsWorks) {
  EXPECT_FALSE(this->pool_.IsIndexInBounds(0, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(0, 1));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 1));

  unsigned char* data = nullptr;
  int size = 0;
  this->pool_.Next(&data, &size);

  for (int i = 0; i < this->block_size_; ++i) {
    EXPECT_TRUE(this->pool_.IsIndexInBounds(0, i));
  }
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 1));

  this->pool_.Next(&data, &size);

  for (int i = 0; i < this->block_size_; ++i) {
    EXPECT_TRUE(this->pool_.IsIndexInBounds(1, i));
  }
  EXPECT_FALSE(this->pool_.IsIndexInBounds(2, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(2, 1));
}


REGISTER_TYPED_TEST_CASE_P(MemoryBlockPoolTest, ReserveWorks,
                           FullBlockSizeReturnWorks, BackupWorks,
                           BackupOverBlockBoundsWorks,
                           RetrieveDataBlockWorks,
                           SizeWorks, IsIndexInBoundsWorks);

typedef ::testing::Types<SizeHolder<10> > Sizes;
INSTANTIATE_TYPED_TEST_CASE_P(Test, MemoryBlockPoolTest, Sizes);
}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
