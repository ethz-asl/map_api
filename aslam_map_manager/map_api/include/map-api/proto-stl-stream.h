#ifndef MAP_API_PROTO_STL_STREAM_H_
#define MAP_API_PROTO_STL_STREAM_H_
#include <memory>
#include <mutex>

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>

namespace map_api {
// This class stores the information at which block and index a serialized
// value starts.
struct MemoryBlockInformation {
  MemoryBlockInformation() : block_index(-1), byte_offset(-1) { }
  int block_index;
  int byte_offset;
};

// This is a block of memory that gets pushed to the container. Basically
// a chunk of memory in the pool.
template <int Size>
struct MemoryBlock {
  MemoryBlock() {
    static_assert(Size > 0, "Block size must be greater than 0.");
  }
  unsigned char data[Size];
};

// This class is the memory pool built up from an STL vector and memory blocks.
// The class is not thread-safe and assumes that only a single
// Zero-Copy-*-Stream operates on it at any time.
// Tracking of where elements begin and end is responsibility of the caller.
template <int BlockSize, template<typename, typename> class Container>
class MemoryBlockPool {
 public:
  typedef MemoryBlock<BlockSize> Block;

  MemoryBlockPool() : block_index_(0),
      position_in_current_block_(0) { }

  // This interface is called by protobuf to get the next buffer to write to.
  bool Next(unsigned char** data, int* size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    int num_available_bytes = BlockSize - position_in_current_block_;
    if (num_available_bytes == 0 || pool_.empty()) {
      if (!pool_.empty()) {
        ++block_index_;
      }
      while (block_index_ >= static_cast<int>(pool_.size())) {
        pool_.push_back(Block());
      }
      position_in_current_block_ = 0;
      num_available_bytes = BlockSize;
    }
    *data = pool_[block_index_].data + position_in_current_block_;
    *size = num_available_bytes;
    position_in_current_block_ += num_available_bytes;
    return true;
  }

  // This is called by protobuf to give data back that was too much.
  void BackUp(int count) {
    while (position_in_current_block_ - count < 0) {
      --block_index_;
      count -= BlockSize;
    }
    position_in_current_block_ -= count;
  }

  // This is called by protobuf to retrieve a block of memory for reading.
  bool RetrieveDataBlock(int index,
                         int byte_offset,
                         const unsigned char** data,
                         int* size) const {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    CHECK_GE(index, 0);
    CHECK_LT(index, static_cast<int>(pool_.size()));
    CHECK_LT(static_cast<int>(byte_offset), BlockSize);
    *data = pool_[index].data + byte_offset;
    *size = BlockSize - byte_offset;
    return true;
  }

  // Helper function to check that access is in bounds.
  bool IsIndexInBounds(int block_index, int position_in_block) const {
    if (block_index < 0 || position_in_block < 0) {
      return false;
    }
    if (block_index < static_cast<int>(pool_.size()) - 1) {
      return true;
    }
    if (block_index == static_cast<int>(pool_.size()) - 1) {
      return position_in_block < position_in_current_block_;
    }
    return false;
  }

  void Reserve(int num_blocks) {
    pool_.reserve(num_blocks);
  }

  int BlockIndex() const {
    return block_index_;
  }

  int PositionInCurrentBlock() const {
    return position_in_current_block_;
  }

  int Size() const {
    return pool_.size();
  }

 private:
  Container<Block, std::allocator<Block> > pool_;
  int block_index_;
  int position_in_current_block_;
};

// This implements a protobuf zero-copy-input stream on top of the memory pool.
template<int BlockSize, template<typename, typename> class Container>
class STLContainerInputStream :
    public google::protobuf::io::ZeroCopyInputStream {
 public:
  STLContainerInputStream(int block_index, int byte_offset,
                          MemoryBlockPool<BlockSize, Container>* block_pool) :
                          block_index_(block_index),
                          byte_offset_(byte_offset),
                          bytes_read_(0),
                          block_pool_(CHECK_NOTNULL(block_pool)) { }

  virtual ~STLContainerInputStream() {}

  // Uses length-prefix framing for protocol buffers.
  bool ReadMessage(
      google::protobuf::Message* message) {
    CHECK_NOTNULL(message);

    // Get the memory where the message size was written to.
    const unsigned char* data = nullptr;
    int size = 0;
    bool status = Next(reinterpret_cast<const void**>(&data), &size);
    if (status == false) {
      return status;
    }
    CHECK_NOTNULL(data);
    // Read the message size.
    google::int32 message_size = 0;
    const int kNumBytesForMessageSizeHeader = sizeof(message_size);
    CHECK(size >= kNumBytesForMessageSizeHeader);
    memcpy(&message_size, data, kNumBytesForMessageSizeHeader);
    CHECK_NE(message_size, 0);

    // Give back excess memory to the pool.
    BackUp(size - kNumBytesForMessageSizeHeader);

    // Now read the message.
    return message->ParseFromBoundedZeroCopyStream(this, message_size);
  }

  // This method is called by protobuf to get the next memory block to read
  // from.
  virtual bool Next(const void ** data, int * size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    bool status = block_pool_->RetrieveDataBlock(
        block_index_, byte_offset_,
        reinterpret_cast<const unsigned char**>(data), size);
    if (status == false) {
      return status;
    }
    bytes_read_ += *size;
    ++block_index_;
    byte_offset_ = 0;
    return status;
  }

  // This method is called by protobuf to return data that was retrieved, but
  // is not needed.
  virtual void BackUp(int count) {
    while (byte_offset_ - count < 0) {
      --block_index_;
      count -= BlockSize;
      bytes_read_ -= BlockSize;
    }
    byte_offset_ -= count;
    bytes_read_ -= count;
  }

  // This method is called by protobuf to skip bytes that are not needed from
  // the input stream.
  virtual bool Skip(int count) {
    while (byte_offset_ + count >= BlockSize) {
      int size_this_block = BlockSize - byte_offset_;
      bytes_read_ += size_this_block;
      count -= size_this_block;
      byte_offset_ = 0;
      ++block_index_;
    }
    byte_offset_ += count;
    bytes_read_ += count;
    return block_pool_->IsIndexInBounds(block_index_, byte_offset_);
  }

  virtual google::int64 ByteCount() const {
    return bytes_read_;
  }

 private:
  int block_index_;
  int byte_offset_;
  google::int64 bytes_read_;
  MemoryBlockPool<BlockSize, Container>* block_pool_;
};

// This implements a protobuf zero-copy-output stream on top of the memory pool.
template<int BlockSize, template<typename, typename> class Container>
class STLContainerOutputStream :
    public google::protobuf::io::ZeroCopyOutputStream {
 public:
  typedef MemoryBlockPool<BlockSize, Container> BlockContainer;
  explicit STLContainerOutputStream(BlockContainer* block_pool) :
                           bytes_written_(0),
                           block_pool_(CHECK_NOTNULL(block_pool)) { }

  ~STLContainerOutputStream() {}

  // Uses length-prefix framing for protocol buffers.
   bool WriteMessage(
      const google::protobuf::Message& message,
      MemoryBlockInformation* block_info) {
     CHECK_NOTNULL(block_info);
    google::int32 message_size = message.ByteSize();
    // Request memory to write the message size to.
    unsigned char* data = nullptr;
    int size = 0;
    // Retrieve block and byte position of the stream. This will be the
    // position that the message will be written to.
    block_info->block_index = BlockIndex();
    block_info->byte_offset = PositionInCurrentBlock();
    const int kNumBytesForMessageSizeHeader = sizeof(message_size);
    while (Next(reinterpret_cast<void**>(&data), &size) == false ||
        size < kNumBytesForMessageSizeHeader) {
      // Update the positions if the last allocation request failed.
      block_info->block_index = BlockIndex();
      block_info->byte_offset = PositionInCurrentBlock();
    }
    CHECK(data != nullptr);
    CHECK(size >= kNumBytesForMessageSizeHeader);
    // Write the message size.
    memcpy(data, &message_size, kNumBytesForMessageSizeHeader);
    // Give back excess memory to the pool.
    BackUp(size - kNumBytesForMessageSizeHeader);

    // Now write the message.
    return message.SerializeToZeroCopyStream(this);
  }

  // This method is called by protobuf to get the next memory block to write to.
  virtual bool Next(void** data, int * size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    bool status = block_pool_->Next(
        reinterpret_cast<unsigned char**>(data), size);
    bytes_written_ += *size;
    return status;
  }

  // This method is called by protobuf to return memory that was retrieved but
  // is not needed.
  virtual void BackUp(int count) {
    block_pool_->BackUp(count);
    bytes_written_ -= count;
  }

  virtual google::int64 ByteCount() const {
    return bytes_written_;
  }

  int BlockIndex() const {
    return block_pool_->BlockIndex();
  }

  int PositionInCurrentBlock() const {
    return block_pool_->PositionInCurrentBlock();
  }

 private:
  google::int64 bytes_written_;
  MemoryBlockPool<BlockSize, Container>* block_pool_;
};

}  // namespace map_api
#endif  // MAP_API_PROTO_STL_STREAM_H_
