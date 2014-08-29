#ifndef VISUAL_INERTIAL_MAPPING_VI_MAP_H_
#define VISUAL_INERTIAL_MAPPING_VI_MAP_H_

#include <unordered_map>

#include <aslam_posegraph/unique-id.h>
#include <aslam_posegraph/pose-graph.h>
#include <Eigen/Dense>
#include <map_api_map_structure/mission.h>
#include <map_api_map_structure/mission-baseframe.h>
#include <viwls_graph/common.h>
#include <viwls_graph/edge.h>
#include <viwls_graph/landmark.h>
#include <viwls_graph/mission.h>
#include <viwls_graph/pose-graph.h>
#include <viwls_graph/unique-id.h>
#include <viwls_graph/vertex.h>

namespace visual_inertial_mapping {
class VIMap {
  friend class VIMapView;

 public:
  typedef Eigen::Matrix<unsigned char, Eigen::Dynamic, 1> DescriptorType;
  typedef std::unordered_map<map_api::map_structure::MissionId, unsigned int>
      MissionToLandmarkCountMap;

  bool hasVertex(const pose_graph::VertexId& id) const;
  viwls_graph::Vertex& getVertex(const pose_graph::VertexId& id);
  const viwls_graph::Vertex& getVertex(const pose_graph::VertexId& id) const;

  bool hasEdge(const pose_graph::EdgeId& id) const;
  viwls_graph::Edge& getEdge(const pose_graph::EdgeId& id);
  const viwls_graph::Edge& getEdge(const pose_graph::EdgeId& id) const;

  bool hasMission(const map_api::map_structure::MissionId& id) const;
  viwls_graph::Mission& getMission(const map_api::map_structure::MissionId& id);
  const viwls_graph::Mission& getMission(
      const map_api::map_structure::MissionId& id) const;

  bool hasMissionBaseFrame(const map_api::map_structure::MissionBaseFrameId& id)
      const;
  map_api::map_structure::MissionBaseFrame& getMissionBaseFrame(
      const map_api::map_structure::MissionBaseFrameId& id);
  const map_api::map_structure::MissionBaseFrame& getMissionBaseFrame(
      const map_api::map_structure::MissionBaseFrameId& id) const;

  bool hasLandmark(const viwls_graph::ImmutableLandmarkId& id) const;
  viwls_graph::Landmark& getLandmark(
      const viwls_graph::ImmutableLandmarkId& id);
  const viwls_graph::Landmark& getLandmark(
      const viwls_graph::ImmutableLandmarkId& id) const;
  void updateLandmarkFromLandmarkVertexTable(
      const viwls_graph::ImmutableLandmarkId& id,
      const viwls_graph::LandmarkId& new_id);

  const viwls_graph::LandmarkAndVertexReference& getVertexReferenceForLandmark(
      const viwls_graph::ImmutableLandmarkId& id) const;
  viwls_graph::Vertex& getVertexForLandmark(
      const viwls_graph::ImmutableLandmarkId& id);
  const viwls_graph::Vertex& getVertexForLandmark(
      const viwls_graph::ImmutableLandmarkId& id) const;

  const map_api::map_structure::MissionId& getMissionIdForVertex(
      const pose_graph::VertexId& id) const;
  const viwls_graph::Mission& getMissionForVertex(
      const pose_graph::VertexId& id) const;
  viwls_graph::Mission& getMissionForVertex(const pose_graph::VertexId& id);

  const map_api::map_structure::MissionId& getMissionIdForLandmark(
      const viwls_graph::ImmutableLandmarkId& id) const;
  const viwls_graph::Mission& getMissionForLandmark(
      const viwls_graph::ImmutableLandmarkId& id) const;
  viwls_graph::Mission& getMissionForLandmark(
      const viwls_graph::ImmutableLandmarkId& id);

  Eigen::Vector3d getLandmark_LM_p_fi(
      const viwls_graph::ImmutableLandmarkId& id) const;
  Eigen::Vector3d getLandmark_G_p_fi(const viwls_graph::ImmutableLandmarkId& id)
      const;

  DescriptorType getMeanLandmarkDescriptor(
      const viwls_graph::ImmutableLandmarkId& id) const;

  void getMissionLandmarkCounts(
      MissionToLandmarkCountMap* mission_to_landmark_count) const;

  void moveLandmarksToOtherVertex(const pose_graph::VertexId& vertex_id_from,
                                  const pose_graph::VertexId& vertex_id_to);

  void sparsifyMission(const map_api::map_structure::MissionId& id,
                       int every_nth_vertex_to_keep);

  bool getNextVertex(const pose_graph::VertexId& current_vertex_id,
                     pose_graph::VertexId* next_vertex_id) const;

  void getAllVertexIds(std::unordered_set<pose_graph::VertexId>* vertices)
      const;

  void getAllEdgeIds(std::unordered_set<pose_graph::EdgeId>* edges) const;

 private:
  void updateVertexReferenceForLandmark(
      const viwls_graph::ImmutableLandmarkId& id,
      const viwls_graph::LandmarkAndVertexReference&);

  viwls_graph::PoseGraph posegraph;
  viwls_graph::MissionMap missions;
  map_api::map_structure::MissionBaseFrameMap mission_base_frames;
  viwls_graph::LandmarkIdToVertexIdMap landmark_to_vertex;
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW
};
}  // namespace visual_inertial_mapping

#endif  // VISUAL_INERTIAL_MAPPING_VI_MAP_H_
