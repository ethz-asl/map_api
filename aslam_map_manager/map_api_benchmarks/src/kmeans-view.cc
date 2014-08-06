#include "map_api_benchmarks/kmeans-view.h"

#include "map_api_benchmarks/app.h"

namespace map_api {
namespace benchmarks{

KmeansView::KmeansView(Chunk* descriptor_chunk, Chunk* center_chunk,
                       Chunk* membership_chunk)
: descriptor_chunk_(CHECK_NOTNULL(descriptor_chunk)),
  center_chunk_(CHECK_NOTNULL(center_chunk)),
  membership_chunk_(CHECK_NOTNULL(membership_chunk)) {}

void KmeansView::insert(const DescriptorVector& descriptors,
                        const DescriptorVector& centers,
                        const std::vector<unsigned int>& memberships) {
  for (size_t i = 0u; i < descriptors.size(); ++i) {
    const DescriptorType& descriptor = descriptors[i];
    std::shared_ptr<Revision> to_insert =
        app::data_point_table->getTemplate();
    Id id;
    common::generateId(&id);
    descriptor_id_to_index_[id] = i;
    descriptor_index_to_id_[i] = id;
    app::descriptorToRevision(descriptor, id, to_insert.get());
    transaction_.insert(app::data_point_table, descriptor_chunk_,
                        to_insert);
  }
  for (size_t i = 0u; i < centers.size(); ++i) {
    const DescriptorType& center = centers[i];
    std::shared_ptr<Revision> to_insert =
        app::center_table->getTemplate();
    Id id;
    common::generateId(&id);
    center_id_to_index_[id] = i;
    center_index_to_id_[i] = id;
    app::centerToRevision(center, id, to_insert.get());
    transaction_.insert(app::center_table, center_chunk_, to_insert);
  }
  for (size_t i = 0u; i < memberships.size(); ++i) {
    unsigned int membership = memberships[i];
    std::shared_ptr<Revision> to_insert =
        app::association_table->getTemplate();
    Id descriptor_id = descriptor_index_to_id_[i];
    Id center_id = center_index_to_id_[membership];
    app::membershipToRevision(descriptor_id, center_id, to_insert.get());
    transaction_.insert(app::association_table, membership_chunk_,
                        to_insert);
  }
  transaction_.commit();
}

void KmeansView::fetch(DescriptorVector* descriptors,
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
  transaction_.find(NetTable::kChunkIdField, descriptor_chunk_->id(),
                    app::data_point_table, &descriptor_revisions_);
  transaction_.find(NetTable::kChunkIdField, center_chunk_->id(),
                    app::center_table, &center_revisions_);
  transaction_.find(NetTable::kChunkIdField, membership_chunk_->id(),
                    app::association_table, &membership_revisions_);

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
  for (const CRTable::RevisionMap::value_type& center :
      center_revisions_) {
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

} /* namespace benchmarks */
} /* namespace map_api */
