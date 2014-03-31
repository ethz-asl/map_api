cmake_minimum_required (VERSION 2.8)
project(map_api)

include(ExternalProject)

find_package(catkin REQUIRED COMPONENTS)

catkin_package(
    DEPENDS eigen
    CATKIN_DEPENDS 
    INCLUDE_DIRS include ${Eigen_INCLUDE_DIRS} ${GLOG_INCLUDE_DIRS} ${GFLAGS_INCLUDE_DIRS}
    LIBRARIES ${PROJECT_NAME}
    CFG_EXTRAS FindGLog.cmake FindGFlags.cmake FindGTest.cmake FindZeroMQ.cmake
)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/bin)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

################
# DEPENDENCIES #
################
include(${PROJECT_SOURCE_DIR}/cmake/dependencies.cmake)
find_package(GLog REQUIRED)
find_package(GFlags REQUIRED)
LIST(APPEND LINK_WITH ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})

############
# PROTOBUF #
############
find_package(Protobuf REQUIRED)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
file (GLOB_RECURSE PROTO_DEFNS proto/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
LIST(APPEND LINK_WITH ${PROTOBUF_LIBRARIES})

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall)

include_directories(include ${catkin_INCLUDE_DIRS}
                    ${ZEROMQ_INCLUDE_DIRS} ${POCO_INCLUDE_DIRS}
                    ${Eigen_INCLUDE_DIRS})

# Core Library available to all applications
SET(CORE_SOURCE src/hash.cc
                src/map-api-core.cc
                src/map-api-hub.cc
                #src/map-manager.cc
                src/table-insert-query.cc
                src/table-interface.cc
                src/posegraph/edge.cc
                src/posegraph/edge-table.cc
                src/posegraph/frame.cc
                src/posegraph/frame-table.cc)
                
add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_HDRS} ${PROTO_SRCS})
add_dependencies(${PROJECT_NAME} ${BUILD_DEPEND})
target_link_libraries(${PROJECT_NAME} ${LINK_WITH})

##########
# GTESTS #
##########

catkin_add_gtest(test_hash_test test/test_main.cpp test/hash_test.cpp)
target_link_libraries(test_hash_test ${PROJECT_NAME} ${LINK_WITH})

#########
# DEMOS #
#########
add_executable(single-posegraph-client demo/single-posegraph-client.cc)
target_link_libraries(single-posegraph-client ${PROJECT_NAME})
