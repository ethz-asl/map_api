// Copyright 2013 Motorola Mobility LLC. Part of the Trailmix project.
// CONFIDENTIAL. AUTHORIZED USE ONLY. DO NOT REDISTRIBUTE.
// This is code written entirely within the trailmix project.
#ifndef MAP_API_BENCHMARKS_HELPERS_H_
#define MAP_API_BENCHMARKS_HELPERS_H_
#include <string>
#include <unordered_map>
#include <thread>
#include <vector>

namespace map_api {
namespace benchmarks {
namespace helpers {
template <int NumBlocks, typename Functor>
void ParallelProcess(size_t num_items, const Functor& functor) {
  constexpr int kNumBlocks = NumBlocks;
  std::vector<std::vector<size_t> > blocks;

  if (num_items < kNumBlocks * 2) {
    blocks.resize(1);
  } else {
    blocks.resize(kNumBlocks);
  }

  const size_t kNumItemsPerBlock = num_items / blocks.size() + 1;

  size_t data_index = 0;

  std::vector<std::thread> threads;
  for (size_t block_idx = 0; block_idx < blocks.size(); ++block_idx) {
    std::vector<size_t>& block = blocks[block_idx];
    for (size_t item_idx = 0u;
         item_idx < kNumItemsPerBlock && data_index < num_items; ++item_idx) {
      block.push_back(data_index);
      ++data_index;
    }
    threads.push_back(
        std::thread([&functor, &block ]()->void { functor(block); }));
  }

  CHECK_EQ(threads.size(), blocks.size());
  for (size_t block_idx = 0; block_idx < blocks.size(); ++block_idx) {
    threads[block_idx].join();
  }
}

}  // namespace helpers
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_HELPERS_H_
