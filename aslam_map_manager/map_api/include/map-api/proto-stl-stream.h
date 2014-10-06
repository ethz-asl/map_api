#ifndef MAP_API_PROTO_STL_STREAM_H_
#define MAP_API_PROTO_STL_STREAM_H_
#include <mutex>

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace map_api {
struct MemoryBlockInformation {
  MemoryBlockInformation() : index(-1), byte_offset(-1) { }
  unsigned int index;
  unsigned int byte_offset;
};

template <int Size>
struct MemoryBlock {
  MemoryBlock() {
    static_assert(Size > 0, "Block size must be greater than 0.");
  }
  unsigned char data[Size];
};

template <int BlockSize, template<typename, typename> class Container>
class MemoryBlockPool {
 public:
  typedef MemoryBlock<BlockSize> Block;

  MemoryBlockPool() : block_index_(0),
      position_in_current_block_(BlockSize) { }

  bool Next(unsigned char** data, int* size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    int num_available_bytes = BlockSize - position_in_current_block_;
    if (num_available_bytes == 0) {
      if (!pool_.empty()) {
        ++block_index_;
        LOG(INFO) << "Block pos now " << block_index_;
      }
      while (block_index_ >= static_cast<int>(pool_.size())) {
        pool_.push_back(Block());
        LOG(INFO) << "Adding block ";
      }
      position_in_current_block_ = 0;
      num_available_bytes = BlockSize;
    }
    LOG(INFO) << "Getting data from block " << block_index_;
    *data = pool_[block_index_].data + position_in_current_block_;
    *size = num_available_bytes;
    position_in_current_block_ += num_available_bytes;
    return true;
  }

  void BackUp(int count) {
    while (position_in_current_block_ - count < 0) {
      --block_index_;
      count -= BlockSize;
    }
    position_in_current_block_ -= count;
  }

  bool RetrieveDataBlock(unsigned int index,
                         unsigned int byte_offset,
                         const unsigned char** data,
                         int* size) const {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    CHECK_LT(index, pool_.size());
    CHECK_LT(static_cast<int>(byte_offset), BlockSize);
    *data = pool_[index].data + byte_offset;
    *size = BlockSize - byte_offset;
    return true;
  }

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

  virtual bool Next(void ** data, int * size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    bool status = block_pool_->RetrieveDataBlock(block_index_, byte_offset_,
                                                 data, size);
    bytes_read_ += *size;
    block_index_ += 1;
    byte_offset_ = 0;
    return status;
  }

  virtual void BackUp(int count) {
    CHECK_LE(count, byte_offset_);
    byte_offset_ -= count;
    bytes_read_ -= count;
  }

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


template<int BlockSize, template<typename, typename> class Container>
class STLContainerOutputStream :
    public google::protobuf::io::ZeroCopyOutputStream {
 public:
  typedef MemoryBlockPool<BlockSize, Container> BlockContainer;
  STLContainerOutputStream(BlockContainer* block_pool) :
                           bytes_written_(0),
                           block_pool_(CHECK_NOTNULL(block_pool)) { }

  ~STLContainerOutputStream() {}

  virtual bool Next(void ** data, int * size) {
    CHECK_NOTNULL(data);
    CHECK_NOTNULL(size);
    bool status = block_pool_->Next(data, size);
    bytes_written_ += *size;
    return status;
  }

  virtual void BackUp(int count) {
    block_pool_->BackUp(count);
    bytes_written_ -= count;
  }

  virtual google::int64 ByteCount() const {
    return bytes_written_;
  }

 private:
  google::int64 bytes_written_;
  MemoryBlockPool<BlockSize, Container>* block_pool_;
};

}  // namespace map_api
#endif  // MAP_API_PROTO_STL_STREAM_H_
