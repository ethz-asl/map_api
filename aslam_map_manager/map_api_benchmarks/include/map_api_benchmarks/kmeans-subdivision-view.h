#ifndef MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_VIEW_H_
#define MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_VIEW_H_

#include <vector>

#include <map-api/chunk.h>
#include <map-api/transaction.h>

#include "map_api_benchmarks/common.h"

namespace map_api {
namespace benchmarks {

/**
 * No time to cleanly merge code with kmeans_view. TODO(tcies) do if it has
 * any point after experiments.
 */
class KmeansSubdivisionView {
 public:
  /**
   * Parameter copy intended.
   */
  KmeansSubdivisionView(size_t degree, double max_dimension, size_t center_num,
                        Chunks center_chunks, Chunks membership_chunks,
                        Chunk* descriptor_chunk);

  /**
   * Does not populate the local revision maps but pushes on straight to the
   * transaction and commits immediately. Meant for insert-only "views".
   */
  void insert(const DescriptorVector& descriptors,
              const DescriptorVector& centers,
              const std::vector<unsigned int>& memberships);

  /**
   * Meant for use in association with updateX()
   */
  void fetch(DescriptorVector* descriptors, DescriptorVector* centers,
             std::vector<unsigned int>* memberships);

  bool updateCenterRelated(size_t chosen_center,
                           const DescriptorVector& centers,
                           const std::vector<unsigned int>& memberships);

 private:
  size_t degree_;
  double max_dimension_;
  size_t center_num_;

  CRTable::RevisionMap descriptor_revisions_;
  CRTable::RevisionMap center_revisions_;
  CRTable::RevisionMap membership_revisions_;

  std::unordered_map<size_t, Id> descriptor_index_to_id_;
  std::unordered_map<size_t, size_t> descriptor_index_to_chunk_index_;
  std::unordered_map<Id, size_t> descriptor_id_to_index_;

  std::unordered_map<size_t, Id> center_index_to_id_;
  std::unordered_map<Id, size_t> center_id_to_index_;

  Chunk* descriptor_chunk_;
  Chunks center_chunks_, membership_chunks_;
  Transaction transaction_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif  // MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_VIEW_H_
