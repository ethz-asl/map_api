cmake_minimum_required (VERSION 2.8)
project(map_api_posegraph)

include(ExternalProject)

find_package(catkin REQUIRED COMPONENTS map_api aslam_posegraph
                                        ceres_error_terms)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)

################
# DEPENDENCIES #
################
# Finding GLog and GFlags is provided by map_api
LIST(APPEND LINK_WITH ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${catkin_LIBRARIES})

############
# PROTOBUF #
############
find_package(Protobuf REQUIRED)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
file (GLOB_RECURSE PROTO_DEFNS proto/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
LIST(APPEND LINK_WITH ${PROTOBUF_LIBRARIES})

######################
# CATKIN DECLARATION #
######################

catkin_package(
    DEPENDS eigen GLog GFlags Protobuf
    CATKIN_DEPENDS map_api aslam_posegraph ceres_error_terms
    INCLUDE_DIRS include ${Eigen_INCLUDE_DIRS} ${GLOG_INCLUDE_DIRS} ${GFLAGS_INCLUDE_DIRS}
    LIBRARIES ${PROJECT_NAME}
)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/bin)

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic)

include_directories(include ${catkin_INCLUDE_DIRS} ${Eigen_INCLUDE_DIRS})

# Core Library available to all applications
SET(PG_SOURCE src/tables/loop-closure-edge-table.cc
              src/tables/odometry-edge-table.cc
              src/tables/vertex-table.cc)
                
add_library(${PROJECT_NAME} ${PG_SOURCE} ${PROTO_HDRS} ${PROTO_SRCS})
target_link_libraries(${PROJECT_NAME} ${LINK_WITH})

##########
# GTESTS #
##########

#catkin_add_gtest(test_simple_test test/test_main.cpp test/simple_test.cpp)
#target_link_libraries(test_simple_test ${PROJECT_NAME} ${LINK_WITH})

## TODO(tcies): Add more tests here.

#########
# DEMOS #
#########
# add_executable(single-posegraph-client demo/single-posegraph-client.cc)
# target_link_libraries(single-posegraph-client ${PROJECT_NAME})
