cmake_minimum_required (VERSION 2.8)
project(map_api_posegraph)

find_package(catkin_simple REQUIRED)

catkin_simple()

################
# DEPENDENCIES #
################
# Finding GLog and GFlags is provided by map_api
LIST(APPEND LINK_WITH ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${catkin_LIBRARIES})
# This is a bit of a hack: We need the core.pb.h file from 
# map_api as include:
include_directories(${CMAKE_BINARY_DIR}/../map_api)

############
# PROTOBUF #
############
include_directories(${CMAKE_CURRENT_BINARY_DIR})
file (GLOB_RECURSE PROTO_DEFNS proto/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
LIST(APPEND LINK_WITH ${PROTOBUF_LIBRARIES})

#############
# LIBRARIES #
#############

add_definitions(--std=c++0x -Wall -Wextra -pedantic)

# Core Library available to all applications
SET(PG_SOURCE src/mission-adder.cc
              src/tables/loop-closure-edge-table.cc
              src/tables/odometry-edge-table.cc
              src/tables/vertex-table.cc)
                
cs_add_library(${PROJECT_NAME} ${PG_SOURCE} ${PROTO_HDRS} ${PROTO_SRCS})
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

##########
# EXPORT #
##########

cs_install()
# we need to add the current binary dir to include dirs due to
# protobuf
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR})