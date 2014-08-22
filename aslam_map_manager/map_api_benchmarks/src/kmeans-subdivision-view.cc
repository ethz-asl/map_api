#include "map_api_benchmarks/kmeans-subdivision-view.h"

#include <sstream>  // NOLINT

#include <glog/logging.h>

#include "map_api_benchmarks/app.h"

namespace map_api {
namespace benchmarks {

KmeansSubdivisionView::KmeansSubdivisionView(size_t degree,
                                             double max_dimension,
                                             size_t center_num,
                                             const Chunks& center_chunks,
                                             const Chunks& membership_chunks,
                                             Chunk* descriptor_chunk)
    : degree_(degree),
      max_dimension_(max_dimension),
      center_num_(center_num),
      descriptor_chunk_(CHECK_NOTNULL(descriptor_chunk)),
      center_chunks_(center_chunks),
      membership_chunks_(membership_chunks) {
  CHECK_EQ(center_num, center_chunks.size());
  CHECK_EQ(degree * degree, membership_chunks.size());
}

void KmeansSubdivisionView::insert(
    const DescriptorVector& descriptors, const DescriptorVector& centers,
    const std::vector<unsigned int>& memberships) {
  std::vector<size_t> distribution(degree_ * degree_);
  for (size_t i = 0u; i < descriptors.size(); ++i) {
    const DescriptorType& descriptor = descriptors[i];
    std::shared_ptr<Revision> to_insert = app::data_point_table->getTemplate();
    Id id;
    common::generateId(&id);
    descriptor_id_to_index_[id] = i;
    descriptor_index_to_id_[i] = id;
    app::descriptorToRevision(descriptor, id, to_insert.get());
    CHECK_NOTNULL(descriptor_chunk_);
    transaction_.insert(app::data_point_table, descriptor_chunk_, to_insert);

    size_t x_sector = ((descriptors[i][0] / max_dimension_) * degree_);
    size_t y_sector = ((descriptors[i][1] / max_dimension_) * degree_);
    size_t chunk_assignment = y_sector + degree_ * x_sector;
    CHECK_LT(chunk_assignment, degree_ * degree_);
    descriptor_index_to_chunk_index_[i] = chunk_assignment;
    ++distribution[chunk_assignment];
  }
  for (size_t i = 0u; i < centers.size(); ++i) {
    const DescriptorType& center = centers[i];
    std::shared_ptr<Revision> to_insert = app::center_table->getTemplate();
    Id id;
    common::generateId(&id);
    center_id_to_index_[id] = i;
    center_index_to_id_[i] = id;
    app::centerToRevision(center, id, to_insert.get());
    CHECK_NOTNULL(center_chunks_[i]);
    transaction_.insert(app::center_table, center_chunks_[i], to_insert);
  }
  for (size_t i = 0u; i < memberships.size(); ++i) {
    unsigned int membership = memberships[i];
    std::shared_ptr<Revision> to_insert = app::association_table->getTemplate();
    Id descriptor_id = descriptor_index_to_id_[i];
    Id center_id = center_index_to_id_[membership];
    app::membershipToRevision(descriptor_id, center_id, to_insert.get());
    CHECK_NOTNULL(membership_chunks_[descriptor_index_to_chunk_index_[i]]);
    transaction_.insert(app::association_table,
                        membership_chunks_[descriptor_index_to_chunk_index_[i]],
                        to_insert);
  }
  transaction_.commit();
  std::ostringstream report;
  report << "Distribution among chunks is: ";
  for (size_t amount : distribution) {
    report << amount << " ";
  }
  LOG(INFO) << report.str();
}

void KmeansSubdivisionView::fetch(DescriptorVector* descriptors,
                                  DescriptorVector* centers,
                                  std::vector<unsigned int>* memberships) {
  CHECK_NOTNULL(descriptors);
  CHECK_NOTNULL(centers);
  CHECK_NOTNULL(memberships);
  descriptors->clear();
  centers->clear();
  memberships->clear();
  descriptor_id_to_index_.clear();
  descriptor_index_to_id_.clear();
  center_id_to_index_.clear();
  center_index_to_id_.clear();

  // cache revisions
  descriptor_revisions_ =
      transaction_.dumpChunk(app::data_point_table, descriptor_chunk_);
  center_revisions_.clear();
  for (Chunk* center_chunk : center_chunks_) {
    CRTable::RevisionMap center_revisions =
        transaction_.dumpChunk(app::center_table, center_chunk);
    center_revisions_.insert(center_revisions.begin(), center_revisions.end());
  }
  membership_revisions_.clear();
  for (Chunk* membership_chunk : membership_chunks_) {
    CRTable::RevisionMap membership_revisions =
        transaction_.dumpChunk(app::association_table, membership_chunk);
    membership_revisions_.insert(membership_revisions.begin(),
                                 membership_revisions.end());
  }

  // construct k-means problem
  int i = 0;
  descriptors->resize(descriptor_revisions_.size());
  for (const CRTable::RevisionMap::value_type& descriptor :
       descriptor_revisions_) {
    Id id;
    descriptor.second->get(CRTable::kIdField, &id);
    descriptor_id_to_index_[id] = i;
    descriptor_index_to_id_[i] = id;
    app::descriptorFromRevision(*descriptor.second, &(*descriptors)[i]);
    ++i;
  }
  i = 0;
  centers->resize(center_revisions_.size());
  for (const CRTable::RevisionMap::value_type& center : center_revisions_) {
    Id id;
    center.second->get(CRTable::kIdField, &id);
    center_id_to_index_[id] = i;
    center_index_to_id_[i] = id;
    app::centerFromRevision(*center.second, &(*centers)[i]);
    ++i;
  }
  memberships->resize(membership_revisions_.size());
  for (const CRTable::RevisionMap::value_type& membership :
       membership_revisions_) {
    Id descriptor_id, center_id;
    app::membershipFromRevision(*membership.second, &descriptor_id, &center_id);
    (*memberships)[descriptor_id_to_index_[descriptor_id]] =
        center_id_to_index_[center_id];
  }
}

bool KmeansSubdivisionView::updateCenterRelated(
    size_t chosen_center, const DescriptorVector& centers,
    const std::vector<unsigned int>& memberships) {
  CHECK_LT(chosen_center, centers.size());
  CHECK_EQ(centers.size(), center_revisions_.size());
  CHECK_EQ(memberships.size(), membership_revisions_.size());

  std::unordered_map<size_t, Id>::iterator found_id =
      center_index_to_id_.find(chosen_center);
  CHECK(found_id != center_index_to_id_.end());
  Id center_id = found_id->second;
  CRTable::RevisionMap::iterator found_revision =
      center_revisions_.find(center_id);
  CHECK(found_revision != center_revisions_.end());
  std::shared_ptr<Revision> cached_revision = found_revision->second;
  app::centerToRevision(centers[chosen_center], center_id,
                        cached_revision.get());
  transaction_.update(app::center_table, cached_revision);

  for (size_t i = 0; i < memberships.size(); ++i) {
    std::unordered_map<size_t, Id>::iterator found_descriptor_id =
        descriptor_index_to_id_.find(i);
    CHECK(found_descriptor_id != descriptor_index_to_id_.end());
    Id descriptor_id = found_descriptor_id->second;

    std::unordered_map<size_t, Id>::iterator found_center_id =
        center_index_to_id_.find(memberships[i]);
    CHECK(found_center_id != center_index_to_id_.end());
    Id center_id = found_center_id->second;

    CRTable::RevisionMap::iterator found_revision =
        membership_revisions_.find(descriptor_id);
    CHECK(found_revision != membership_revisions_.end());
    std::shared_ptr<Revision> cached_revision = found_revision->second;

    Id former_center_id;
    cached_revision->get(app::kAssociationTableCenterIdField,
                         &former_center_id);
    size_t former_center_index = center_id_to_index_[former_center_id];

    // update coloring only if a descriptor has previously been assigned to
    // the chosen center or is newly assigned to it
    if (memberships[i] == chosen_center ||
        former_center_index == chosen_center) {
      app::membershipToRevision(descriptor_id, center_id,
                                cached_revision.get());
      transaction_.update(app::association_table, cached_revision);
    }
  }
  return transaction_.commit();
}

} /* namespace benchmarks */
} /* namespace map_api */
