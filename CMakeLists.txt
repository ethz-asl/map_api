cmake_minimum_required(VERSION 2.8.3)
project(aslam_posegraph)

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/)

find_package(catkin_simple REQUIRED)

catkin_simple()

###########
## Build ##
###########

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-parameter")
if (CMAKE_COMPILER_IS_GNUCC)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

cs_add_library(${PROJECT_NAME} src/edge.cc src/vertex.cc src/pose-graph.cc 
	src/pose-vertex.cc src/loop-closure-edge.cc src/odometry-edge.cc
	src/hash-id.cc src/switchable-constraints-pose-graph.cc)

#############
## Testing ##
#############

catkin_add_gtest(test_posegraph test/test_posegraph.cc)
target_link_libraries(test_posegraph ${PROJECT_NAME} ${GLOG_LIBRARY})

cs_export()
