#include <visual_inertial_map_structure/vi-map.h>
#include <viwls_graph/descriptor-utils.h>
#include <viwls_graph/vertex.h>

namespace visual_inertial_mapping {
bool VIMap::hasVertex(const pose_graph::VertexId& id) const {
  return posegraph.vertexExists(id);
}
viwls_graph::Vertex& VIMap::getVertex(const pose_graph::VertexId& id) {
  return posegraph.getVertexPtr(id)->getAs<viwls_graph::Vertex>();
}
const viwls_graph::Vertex& VIMap::getVertex(const pose_graph::VertexId& id)
    const {
  return posegraph.getVertexPtr(id)->getAs<const viwls_graph::Vertex>();
}

bool VIMap::hasEdge(const pose_graph::EdgeId& id) const {
  return posegraph.edgeExists(id);
}
viwls_graph::Edge& VIMap::getEdge(const pose_graph::EdgeId& id) {
  return posegraph.getEdgePtr(id)->getAs<viwls_graph::Edge>();
}
const viwls_graph::Edge& VIMap::getEdge(const pose_graph::EdgeId& id) const {
  return posegraph.getEdgePtr(id)->getAs<const viwls_graph::Edge>();
}

bool VIMap::hasMission(const map_api::map_structure::MissionId& id) const {
  return missions.find(id) != missions.end();
}
viwls_graph::Mission& VIMap::getMission(
    const map_api::map_structure::MissionId& id) {
  viwls_graph::MissionMap::iterator it = missions.find(id);
  CHECK(it != missions.end()) << "Mission " << id.hexString() << " not found";
  return it->second->getAs<viwls_graph::Mission>();
}
const viwls_graph::Mission& VIMap::getMission(
    const map_api::map_structure::MissionId& id) const {
  viwls_graph::MissionMap::const_iterator it = missions.find(id);
  CHECK(it != missions.end()) << "Mission " << id.hexString() << " not found";
  return it->second->getAs<const viwls_graph::Mission>();
}

bool VIMap::hasMissionBaseFrame(
    const map_api::map_structure::MissionBaseFrameId& id) const {
  return mission_base_frames.find(id) != mission_base_frames.end();
}
map_api::map_structure::MissionBaseFrame& VIMap::getMissionBaseFrame(
    const map_api::map_structure::MissionBaseFrameId& id) {
  map_api::map_structure::MissionBaseFrameMap::iterator it =
      mission_base_frames.find(id);
  CHECK(it != mission_base_frames.end()) << "MissionBaseFrame "
                                         << id.hexString() << " not found";
  return *it->second;
}
const map_api::map_structure::MissionBaseFrame& VIMap::getMissionBaseFrame(
    const map_api::map_structure::MissionBaseFrameId& id) const {
  map_api::map_structure::MissionBaseFrameMap::const_iterator it =
      mission_base_frames.find(id);
  CHECK(it != mission_base_frames.end()) << "MissionBaseFrame "
                                         << id.hexString() << " not found";
  return *it->second;
}

bool VIMap::hasLandmark(const viwls_graph::ImmutableLandmarkId& id) const {
  viwls_graph::LandmarkIdToVertexIdMap::const_iterator it =
      landmark_to_vertex.find(id);
  return it != landmark_to_vertex.end();
}

void VIMap::updateLandmarkFromLandmarkVertexTable(
    const viwls_graph::ImmutableLandmarkId& id,
    const viwls_graph::LandmarkId& new_id) {
  CHECK(hasLandmark(id));
  viwls_graph::LandmarkIdToVertexIdMap::const_iterator it =
      landmark_to_vertex.find(id);
  CHECK(it != landmark_to_vertex.end());
  viwls_graph::LandmarkAndVertexReference& ref = it->second;
  ref.landmark_id = new_id;
}

viwls_graph::Landmark& VIMap::getLandmark(
    const viwls_graph::ImmutableLandmarkId& id) {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  viwls_graph::Vertex& vertex = getVertex(vertex_ref.vertex_id);
  return vertex.getLandmarks().getLandmark(vertex_ref.landmark_id);
}
const viwls_graph::Landmark& VIMap::getLandmark(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  const viwls_graph::Vertex& vertex = getVertex(vertex_ref.vertex_id);
  return vertex.getLandmarks().getLandmark(vertex_ref.landmark_id);
}

const viwls_graph::LandmarkAndVertexReference&
VIMap::getVertexReferenceForLandmark(const viwls_graph::ImmutableLandmarkId& id)
    const {
  viwls_graph::LandmarkIdToVertexIdMap::const_iterator it =
      landmark_to_vertex.find(id);
  CHECK(it != landmark_to_vertex.end());
  return it->second;
}
viwls_graph::Vertex& VIMap::getVertexForLandmark(
    const viwls_graph::ImmutableLandmarkId& id) {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  return getVertex(vertex_ref.vertex_id);
}

const viwls_graph::Vertex& VIMap::getVertexForLandmark(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  return getVertex(vertex_ref.vertex_id);
}

const map_api::map_structure::MissionId& VIMap::getMissionIdForVertex(
    const pose_graph::VertexId& id) const {
  const viwls_graph::Vertex& vertex = getVertex(id);
  return vertex.getMissionId();
}
const viwls_graph::Mission& VIMap::getMissionForVertex(
    const pose_graph::VertexId& id) const {
  const map_api::map_structure::MissionId mission_id =
      getMissionIdForVertex(id);
  return getMission(mission_id);
}
viwls_graph::Mission& VIMap::getMissionForVertex(
    const pose_graph::VertexId& id) {
  const map_api::map_structure::MissionId mission_id =
      getMissionIdForVertex(id);
  return getMission(mission_id);
}

const map_api::map_structure::MissionId& VIMap::getMissionIdForLandmark(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::Vertex& vertex = getVertexForLandmark(id);
  return vertex.getMissionId();
}
const viwls_graph::Mission& VIMap::getMissionForLandmark(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const map_api::map_structure::MissionId& mission_id =
      getMissionIdForLandmark(id);
  return getMission(mission_id);
}
viwls_graph::Mission& VIMap::getMissionForLandmark(
    const viwls_graph::ImmutableLandmarkId& id) {
  const map_api::map_structure::MissionId& mission_id =
      getMissionIdForLandmark(id);
  return getMission(mission_id);
}

Eigen::Vector3d VIMap::getLandmark_LM_p_fi(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  const viwls_graph::Vertex& vertex = getVertex(vertex_ref.vertex_id);
  return vertex.getLandmark_LM_p_fi(vertex_ref.landmark_id).toImplementation();
}

Eigen::Vector3d VIMap::getLandmark_G_p_fi(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::LandmarkAndVertexReference& vertex_ref =
      getVertexReferenceForLandmark(id);
  const viwls_graph::Vertex& vertex = getVertex(vertex_ref.vertex_id);
  pose::Position3D LM_p_fi = vertex.getLandmark_LM_p_fi(vertex_ref.landmark_id);

  const viwls_graph::Mission& mission = getMission(vertex.getMissionId());
  const map_api::map_structure::MissionBaseFrame& mission_baseframe =
      VIMap::getMissionBaseFrame(mission.getBaseFrameId());

  return mission_baseframe.transformPointInMissionFrameToGlobalFrame(
      LM_p_fi.toImplementation());
}

VIMap::DescriptorType VIMap::getMeanLandmarkDescriptor(
    const viwls_graph::ImmutableLandmarkId& id) const {
  const viwls_graph::Landmark& landmark = getLandmark(id);

  std::vector<const DescriptorType*> descriptor_ptrs;
  for (const std::pair<pose_graph::VertexId, int>& keyframe :
       landmark.vertices_) {
    const DescriptorType& descriptor =
        getVertex(keyframe.first).getVisualFrame().getBriskDescriptors().col(
            keyframe.second);
    descriptor_ptrs.push_back(&descriptor);
  }

  DescriptorType mean;
  viwls_graph::descriptorMean(descriptor_ptrs, &mean);

  return mean;
}

void VIMap::getMissionLandmarkCounts(
    MissionToLandmarkCountMap* mission_to_landmark_count) const {
  CHECK_NOTNULL(mission_to_landmark_count);

  // Put missions with zero landmark counts on the map.
  for (const viwls_graph::MissionMap::value_type& mission : missions) {
    mission_to_landmark_count->insert(std::make_pair(mission.second->id(), 0));
  }

  // Count landmarks by iterating through vertices/landmark stores.
  std::unordered_set<pose_graph::VertexId> vertex_ids;
  posegraph.getAllVertexIds(&vertex_ids);
  for (const pose_graph::VertexId& vertex_id : vertex_ids) {
    const viwls_graph::Vertex& vertex = getVertex(vertex_id);
    const viwls_graph::LandmarkStore& landmark_store = vertex.getLandmarks();

    CHECK_GT(mission_to_landmark_count->count(vertex.getMissionId()), 0u);
    (*mission_to_landmark_count)[vertex.getMissionId()] +=
        landmark_store.size();
  }
}

void VIMap::updateVertexReferenceForLandmark(
    const viwls_graph::ImmutableLandmarkId& id,
    const viwls_graph::LandmarkAndVertexReference& reference) {
  viwls_graph::LandmarkIdToVertexIdMap::iterator it =
      landmark_to_vertex.find(id);
  CHECK(it != landmark_to_vertex.end());
  it->second = reference;
}

void VIMap::getAllVertexIds(std::unordered_set<pose_graph::VertexId>* vertices)
    const {
  return posegraph.getAllVertexIds(vertices);
}

void VIMap::getAllEdgeIds(std::unordered_set<pose_graph::EdgeId>* edges) const {
  return posegraph.getAllEdgeIds(edges);
}

bool VIMap::getNextVertex(const pose_graph::VertexId& current_vertex_id,
                          pose_graph::VertexId* next_vertex_id) const {
  CHECK_NOTNULL(next_vertex_id);

  std::unordered_set<pose_graph::EdgeId> outgoing_edges;
  getVertex(current_vertex_id).outgoingEdges(&outgoing_edges);

  if (!outgoing_edges.empty()) {
    pose_graph::EdgeId edge_to_next_id = *(outgoing_edges.begin());
    *next_vertex_id = getEdge(edge_to_next_id).to();
    return true;
  }
  return false;
}

void VIMap::sparsifyMission(const map_api::map_structure::MissionId& id,
                            int every_nth_vertex_to_keep) {
  CHECK_GT(every_nth_vertex_to_keep, 0);
  const viwls_graph::Mission& mission = getMission(id);
  const pose_graph::VertexId& root_vertex_id = mission.getRootVertexId();

  pose_graph::VertexId kept_vertex_id = root_vertex_id;
  pose_graph::VertexId current_vertex_id;

  do {
    current_vertex_id = kept_vertex_id;
    VLOG(3) << "Vertex " << current_vertex_id.hexString()
            << " : landmark count: "
            << getVertex(current_vertex_id).getLandmarks().size();
    for (int i = 0; i < (every_nth_vertex_to_keep - 1); ++i) {
      if (getNextVertex(current_vertex_id, &current_vertex_id)) {
        moveLandmarksToOtherVertex(current_vertex_id, kept_vertex_id);
        VLOG(3) << "Vertex " << current_vertex_id.hexString()
                << " : landmark count: "
                << getVertex(current_vertex_id).getLandmarks().size();
      } else {
        break;
      }
    }
  } while (getNextVertex(current_vertex_id, &kept_vertex_id));
}

void VIMap::moveLandmarksToOtherVertex(
    const pose_graph::VertexId& vertex_id_from,
    const pose_graph::VertexId& vertex_id_to) {
  CHECK_NE(vertex_id_from.hexString(), vertex_id_to.hexString());

  viwls_graph::Vertex& vertex_from = getVertex(vertex_id_from);
  viwls_graph::Vertex& vertex_to = getVertex(vertex_id_to);

  viwls_graph::LandmarkStore& landmark_store_from = vertex_from.getLandmarks();
  viwls_graph::LandmarkStore& landmark_store_to = vertex_to.getLandmarks();

  const Eigen::Vector3d& M_p_I =
      vertex_to.getTransformation().getPosition().toImplementation();
  const Eigen::Quaterniond& M_q_I =
      vertex_to.getTransformation().getRotation().toImplementation();
  Eigen::Matrix3d M_R_I, I_R_M;
  common::toRotationMatrix(M_q_I.coeffs(), &M_R_I);
  I_R_M = M_R_I.transpose();

  std::unordered_set<viwls_graph::LandmarkId> landmarks_to_be_removed;

  for (const viwls_graph::Landmark& landmark : landmark_store_from) {
    viwls_graph::LandmarkId landmark_id = landmark.id();
    viwls_graph::ImmutableLandmarkId immutable_landmark_id;
    immutable_landmark_id.fromLandmarkId(landmark_id);

    // TODO(dymczykm) If slow then use the local reference to vertex_from.
    const Eigen::Vector3d& G_p_fi = getLandmark_G_p_fi(immutable_landmark_id);
    const map_api::map_structure::MissionBaseFrame& mission_baseframe_to =
        getMissionBaseFrame(getMissionForVertex(vertex_id_to).getBaseFrameId());

    const Eigen::Vector3d M_p_fi =
        mission_baseframe_to.transformPointInGlobalFrameToMissionFrame(G_p_fi);
    const Eigen::Vector3d I_p_fi = I_R_M * (M_p_fi - M_p_I);

    Eigen::Matrix3d covariance = Eigen::Matrix3d::Identity();

    CHECK(!landmark_store_to.hasLandmark(landmark_id));
    landmark_store_to.addLandmark(landmark);
    viwls_graph::Landmark& new_landmark =
        landmark_store_to.getLandmark(landmark_id);
    new_landmark.setPosition(pose::Position3D(I_p_fi));
    new_landmark.setCovariance(covariance);

    // We should not keep dead landmarks in LandmarkStore.
    CHECK(getVertexReferenceForLandmark(immutable_landmark_id)
              .landmark_id == landmark_id)
        << "Probably a dead landmark in landmark store: "
        << landmark.id().hexString() << " in landmark store, but "
        << getVertexReferenceForLandmark(immutable_landmark_id)
               .landmark_id.hexString()
        << " stored in landmark-vertex reference table.";

    viwls_graph::LandmarkAndVertexReference new_reference;
    new_reference.vertex_id = vertex_id_to;
    new_reference.landmark_id = landmark_id;
    updateVertexReferenceForLandmark(immutable_landmark_id, new_reference);

    landmarks_to_be_removed.insert(landmark_id);
  }

  for (const viwls_graph::LandmarkId& landmark_id : landmarks_to_be_removed) {
    landmark_store_from.removeLandmark(landmark_id);
  }
}

}  // namespace visual_inertial_mapping
