#include "map_api_posegraph/mission-adder.h"
#include "map_api_posegraph/objects/odometry-edge.h"
#include "posegraph.pb.h"

#include <Eigen/Core>

using namespace map_api::posegraph;

bool createProtosOf7NodesUnmatched(
    std::vector<proto::LoopClosureEdge>& loopClosureEdges,
    std::vector<proto::OdometryEdge>& odometryEdges,
    std::vector<proto::Vertex>& vertices) {
  // Vertices
  vertices.resize(7);


  // Odometry edges
  Eigen::Matrix<double, 6, 6> dummy_covariance;
  dummy_covariance.setIdentity();
  Eigen::Matrix<double, 6, 6> large_rotation_covariance = dummy_covariance;
  large_rotation_covariance(3,3) = 1e10;
  large_rotation_covariance(4,4) = 1e10;
  large_rotation_covariance(5,5) = 1e10;


}

