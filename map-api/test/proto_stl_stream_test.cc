#include <vector>

#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/gzip_stream.h>
#include <gtest/gtest.h>
#include <stxxl.h>

#include "map-api/proto-stl-stream.h"
#include "map-api/revision.h"
#include "map-api/test/testing-entrypoint.h"
#include "./core.pb.h"

namespace map_api {
template<int Size>
struct SizeHolder {
  enum {
    value = Size
  };
};

template<class T, class A> using ContainerType =
    typename stxxl::VECTOR_GENERATOR<T>::result;

// Using typed tests to be able to infer the integer block size at compile time.
template<typename SizeHolder_T>
class ProtoSTLStream : public ::testing::Test {
 protected:
  typedef MemoryBlockPool<SizeHolder_T::value, ContainerType> PoolType;

  virtual void SetUp() {
    stats_begin_ = stxxl::stats_data(*stxxl::stats::get_instance());
  }

  virtual void TearDown() {
    // Print STXXL stats.
    LOG(INFO) << (stxxl::stats_data(*stxxl::stats::get_instance()) -
        stats_begin_);
  }

  PoolType pool_;
  stxxl::stats_data stats_begin_;
};

TYPED_TEST_CASE_P(ProtoSTLStream);

TYPED_TEST_P(ProtoSTLStream, ReserveWorks) {
  this->pool_.Reserve(10);
}

TYPED_TEST_P(ProtoSTLStream, FullBlockSizeReturnWorks) {
  unsigned char* data = nullptr;
  int size = 0;
  EXPECT_EQ(0, this->pool_.BlockIndex());
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());

  this->pool_.Next(&data, &size);

  EXPECT_NE(data, nullptr);
  EXPECT_EQ(TypeParam::value, size);
  EXPECT_EQ(1, this->pool_.BlockIndex());
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());
}

TYPED_TEST_P(ProtoSTLStream, BackupWorks) {
  unsigned char* data1 = nullptr;
  int size = 0;
  this->pool_.Next(&data1, &size);

  EXPECT_NE(data1, nullptr);
  EXPECT_EQ(TypeParam::value, size);

  this->pool_.BackUp(2);
  EXPECT_EQ(TypeParam::value - 2, this->pool_.PositionInCurrentBlock());

  unsigned char* data2 = nullptr;
  this->pool_.Next(&data2, &size);

  EXPECT_EQ(data1 + TypeParam::value - 2, data2);
  EXPECT_EQ(2, size);
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());
}

TYPED_TEST_P(ProtoSTLStream, BackupOverBlockBoundsWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  unsigned char* data1 = nullptr;
  int size = 0;
  this->pool_.Next(&data1, &size);
  EXPECT_EQ(1, this->pool_.BlockIndex());
  unsigned char* data2 = nullptr;
  this->pool_.Next(&data2, &size);
  EXPECT_EQ(2, this->pool_.BlockIndex());

  this->pool_.BackUp(TypeParam::value + 2);
  EXPECT_EQ(TypeParam::value - 2, this->pool_.PositionInCurrentBlock());
  EXPECT_EQ(0, this->pool_.BlockIndex());

  unsigned char* data3 = nullptr;
  this->pool_.Next(&data3, &size);

  EXPECT_EQ(1, this->pool_.BlockIndex());
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());
  EXPECT_EQ(data1 + TypeParam::value - 2, data3);
  EXPECT_EQ(2, size);
}

TYPED_TEST_P(ProtoSTLStream, RetrieveDataBlockWorks) {
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
  EXPECT_EQ(TypeParam::value, size_get);
  EXPECT_EQ(data1, data_get);

  this->pool_.RetrieveDataBlock(0, 1, &data_get, &size_get);
  EXPECT_EQ(TypeParam::value - 1, size_get);
  EXPECT_EQ(data1 + 1, data_get);

  this->pool_.RetrieveDataBlock(1, 0, &data_get, &size_get);
  EXPECT_EQ(TypeParam::value, size_get);
  EXPECT_EQ(data2, data_get);

  this->pool_.RetrieveDataBlock(1, 2, &data_get, &size_get);
  EXPECT_EQ(TypeParam::value - 2, size_get);
  EXPECT_EQ(data2 + 2, data_get);
}

TYPED_TEST_P(ProtoSTLStream, SizeWorks) {
  EXPECT_EQ(this->pool_.Size(), 0);
  unsigned char* data = nullptr;
  int size = 0;
  this->pool_.Next(&data, &size);
  EXPECT_EQ(this->pool_.Size(), 2);
  this->pool_.Next(&data, &size);
  EXPECT_EQ(this->pool_.Size(), 3);
}

TYPED_TEST_P(ProtoSTLStream, IsIndexInBoundsWorks) {
  EXPECT_FALSE(this->pool_.IsIndexInBounds(0, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(0, 1));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 1));

  unsigned char* data = nullptr;
  int size = 0;
  this->pool_.Next(&data, &size);

  for (int i = 0; i < TypeParam::value; ++i) {
    EXPECT_TRUE(this->pool_.IsIndexInBounds(0, i));
  }
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(1, 1));

  this->pool_.Next(&data, &size);

  for (int i = 0; i < TypeParam::value; ++i) {
    EXPECT_TRUE(this->pool_.IsIndexInBounds(1, i));
  }
  EXPECT_FALSE(this->pool_.IsIndexInBounds(2, 0));
  EXPECT_FALSE(this->pool_.IsIndexInBounds(2, 1));
}

TYPED_TEST_P(ProtoSTLStream, OutputStreamByteCountWorks) {
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  EXPECT_EQ(0, output_stream.ByteCount());
  unsigned char* data = nullptr;
  int size = 0;
  output_stream.Next(reinterpret_cast<void**>(&data), &size);

  EXPECT_EQ(TypeParam::value, output_stream.ByteCount());

  output_stream.BackUp(2);

  EXPECT_EQ(TypeParam::value - 2, output_stream.ByteCount());
}

TYPED_TEST_P(ProtoSTLStream, OutputStreamNextWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data0 = nullptr;
  int size0 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data0), &size0);

  EXPECT_NE(data0, nullptr);
  EXPECT_EQ(TypeParam::value, size0);
  EXPECT_EQ(1, this->pool_.BlockIndex());
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());

  unsigned char* data1 = nullptr;
  int size1 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data1), &size1);

  EXPECT_EQ(TypeParam::value, size1);
  EXPECT_EQ(data0 + TypeParam::value, data1);
}

TYPED_TEST_P(ProtoSTLStream, OutputStreamBackUpWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data0 = nullptr;
  int size0 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data0), &size0);

  this->pool_.BackUp(2);
  EXPECT_EQ(TypeParam::value - 2, this->pool_.PositionInCurrentBlock());

  unsigned char* data1 = nullptr;
  int size1 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data1), &size1);

  EXPECT_EQ(data0 + TypeParam::value - 2, data1);
  EXPECT_EQ(2, size1);
  EXPECT_EQ(0, this->pool_.PositionInCurrentBlock());
}

TYPED_TEST_P(ProtoSTLStream, InputStreamByteCountWorks) {
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data_out = nullptr;
  int size_out = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out), &size_out);
  output_stream.Next(reinterpret_cast<void**>(&data_out), &size_out);

  STLContainerInputStream<TypeParam::value, ContainerType> input_stream(
      0, 0, &this->pool_);

  EXPECT_EQ(0, input_stream.ByteCount());

  const unsigned char* data_in = nullptr;
  int size_in = 0;
  input_stream.Next(reinterpret_cast<const void**>(&data_in), &size_in);

  EXPECT_EQ(TypeParam::value, input_stream.ByteCount());

  input_stream.BackUp(2);

  EXPECT_EQ(TypeParam::value - 2, input_stream.ByteCount());
}

TYPED_TEST_P(ProtoSTLStream, InputStreamNextWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data_out_0 = nullptr;
  int size_out_0 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_0), &size_out_0);
  unsigned char* data_out_1 = nullptr;
  int size_out_1 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_1), &size_out_1);

  // At block beginning.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream0(
      0, 0, &this->pool_);
  const unsigned char* data_in = nullptr;
  int size_in = 0;
  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(data_out_0, data_in);
  EXPECT_EQ(TypeParam::value, size_in);

  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);

  EXPECT_EQ(data_out_1, data_in);
  EXPECT_EQ(TypeParam::value, size_in);

  // With offset.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream1(
      0, 2, &this->pool_);
  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(data_out_0 + 2, data_in);
  EXPECT_EQ(TypeParam::value - 2, size_in);

  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);

  EXPECT_EQ(data_out_1, data_in);
  EXPECT_EQ(TypeParam::value, size_in);
}

TYPED_TEST_P(ProtoSTLStream, InputStreamBackUpWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data_out_0 = nullptr;
  int size_out_0 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_0), &size_out_0);
  unsigned char* data_out_1 = nullptr;
  int size_out_1 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_1), &size_out_1);

  STLContainerInputStream<TypeParam::value, ContainerType> input_stream0(
      0, 0, &this->pool_);
  const unsigned char* data_in = nullptr;
  int size_in = 0;
  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  input_stream0.BackUp(2);
  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(2, size_in);
  EXPECT_EQ(data_out_0 + TypeParam::value - 2, data_in);

  // Over block bounds.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream1(
      0, 0, &this->pool_);
  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  input_stream1.BackUp(TypeParam::value + 2);
  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(2, size_in);
  EXPECT_EQ(data_out_0 + TypeParam::value - 2, data_in);
}

TYPED_TEST_P(ProtoSTLStream, InputStreamSkipWorks) {
  // Avoid reallocation, s.t. memory adr can be compared.
  this->pool_.Reserve(2);
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);
  unsigned char* data_out_0 = nullptr;
  int size_out_0 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_0), &size_out_0);
  unsigned char* data_out_1 = nullptr;
  int size_out_1 = 0;
  output_stream.Next(reinterpret_cast<void**>(&data_out_1), &size_out_1);

  STLContainerInputStream<TypeParam::value, ContainerType> input_stream0(
      0, 0, &this->pool_);
  const unsigned char* data_in = nullptr;
  int size_in = 0;
  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  input_stream0.Skip(2);
  input_stream0.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(TypeParam::value - 2, size_in);
  EXPECT_EQ(data_out_1 + 2, data_in);

  // Over block bounds.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream1(
      0, 0, &this->pool_);
  input_stream1.Skip(TypeParam::value + 2);
  input_stream1.Next(reinterpret_cast<const void**>(&data_in), &size_in);
  EXPECT_EQ(TypeParam::value - 2, size_in);
  EXPECT_EQ(data_out_1 + 2, data_in);
}

TYPED_TEST_P(ProtoSTLStream, ProtoManualSerializationWorks) {
  std::shared_ptr<map_api::Revision> revision_out = Revision::fromProto(
      std::unique_ptr<proto::Revision>(new proto::Revision));

  revision_out->addField<std::string>(0);
  revision_out->addField<int>(1);
  revision_out->addField<double>(2);
  revision_out->addField<double>(3);

  revision_out->set(0, std::string("test"));
  revision_out->set(1, 42);
  revision_out->set(2, 3.14);
  revision_out->set(3, 6.28);

  // Write the data to the blocks.
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);

  int block_index = output_stream.BlockIndex();
  int position_in_block = output_stream.PositionInCurrentBlock();

  int msg_size_out = revision_out->byteSize();
  {
    google::protobuf::io::CodedOutputStream coded_out(&output_stream);
    coded_out.WriteVarint32(msg_size_out);
    revision_out->SerializeToCodedStream(&coded_out);
  }
  EXPECT_EQ((msg_size_out + 1), output_stream.ByteCount());
  EXPECT_EQ((msg_size_out + 1) / TypeParam::value, output_stream.BlockIndex());
  EXPECT_EQ((msg_size_out + 1) % TypeParam::value,
            output_stream.PositionInCurrentBlock());

  // Read the data back in.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream(
      block_index, position_in_block, &this->pool_);
  google::protobuf::io::CodedInputStream coded_in(&input_stream);
  uint32_t msg_size_in;
  coded_in.SetTotalBytesLimit(msg_size_out + sizeof(msg_size_in),
                              msg_size_out + sizeof(msg_size_in));

  ASSERT_TRUE(coded_in.ReadVarint32(&msg_size_in));
  std::string input_string;
  ASSERT_TRUE(coded_in.ReadString(&input_string, msg_size_in));

  std::shared_ptr<Revision> revision_in =
      Revision::fromProtoString(input_string);

  EXPECT_TRUE(*revision_in == *revision_out);
}


TYPED_TEST_P(ProtoSTLStream, ProtoAutoSerializationWorks) {
  std::shared_ptr<map_api::Revision> revision_out = Revision::fromProto(
      std::unique_ptr<proto::Revision>(new proto::Revision));

  revision_out->addField<std::string>(0);
  revision_out->addField<int>(1);
  revision_out->addField<double>(2);
  revision_out->addField<double>(3);

  revision_out->set(0, std::string("test"));
  revision_out->set(1, 42);
  revision_out->set(2, 3.14);
  revision_out->set(3, 6.28);

  // Write the data to the blocks.
  STLContainerOutputStream<TypeParam::value, ContainerType> output_stream(
      &this->pool_);

  MemoryBlockInformation block_information;
  output_stream.WriteMessage(*revision_out->underlying_revision_,
                             &block_information);
  EXPECT_EQ(block_information.block_index, 0);
  EXPECT_EQ(block_information.byte_offset, 0);

  EXPECT_EQ(output_stream.ByteCount(),
            static_cast<int>(revision_out->underlying_revision_->ByteSize() +
                             sizeof(google::int32)));

  // Read the data back in.
  STLContainerInputStream<TypeParam::value, ContainerType> input_stream(
      block_information.block_index, block_information.byte_offset,
      &this->pool_);

  std::unique_ptr<proto::Revision> proto_in(new proto::Revision);
  input_stream.ReadMessage(proto_in.get());
  std::shared_ptr<Revision> revision_in =
      Revision::fromProto(std::move(proto_in));

  EXPECT_TRUE(*revision_in == *revision_out);
}

REGISTER_TYPED_TEST_CASE_P(ProtoSTLStream, ReserveWorks,
                           FullBlockSizeReturnWorks, BackupWorks,
                           BackupOverBlockBoundsWorks,
                           RetrieveDataBlockWorks,
                           SizeWorks, IsIndexInBoundsWorks,
                           OutputStreamByteCountWorks,
                           OutputStreamNextWorks,
                           OutputStreamBackUpWorks,
                           InputStreamByteCountWorks,
                           InputStreamNextWorks,
                           InputStreamBackUpWorks,
                           InputStreamSkipWorks,
                           ProtoManualSerializationWorks,
                           ProtoAutoSerializationWorks);

typedef ::testing::Types<SizeHolder<5>, SizeHolder<10>,
    SizeHolder<50>, SizeHolder<100>, SizeHolder<1024>, SizeHolder<2048>,
    SizeHolder<2047>, SizeHolder<2049>
> Sizes;

INSTANTIATE_TYPED_TEST_CASE_P(Test, ProtoSTLStream, Sizes);

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
