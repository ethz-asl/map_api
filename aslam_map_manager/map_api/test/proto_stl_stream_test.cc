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
  this->pool_.Next(&data, &size);
  EXPECT_NE(data, nullptr);
  EXPECT_NE(size, 0);
  EXPECT_EQ(this->block_size_, size);
}

REGISTER_TYPED_TEST_CASE_P(MemoryBlockPoolTest, ReserveWorks,
                           FullBlockSizeReturnWorks);

typedef ::testing::Types<SizeHolder<10> > Sizes;
INSTANTIATE_TYPED_TEST_CASE_P(Test, MemoryBlockPoolTest, Sizes);
}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
