cmake_minimum_required (VERSION 2.8)
project(map_api_benchmarks)

find_package(catkin_simple REQUIRED)

catkin_simple()

############
# PROTOBUF #
############
set(PROTO_DEFNS proto/kmeans.proto)
PROTOBUF_CATKIN_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic)

cs_add_library(${PROJECT_NAME} ${PROTO_SRCS} src/data-point.cc src/center.cc
               src/association.cc src/app.cc)

# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} gflags_catkin glog_catkin)

#cs_add_executable(kmeans_benchmark src/kmeans_benchmark.cc)
#target_link_libraries(kmeans_benchmark ${PROJECT_NAME})

catkin_add_gtest(test_kmeans test/test_kmeans.cc)
target_link_libraries(test_kmeans ${PROJECT_NAME})

cs_install()
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR})