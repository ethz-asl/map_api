cmake_minimum_required (VERSION 2.8)
project(dmap_common)

find_package(catkin_simple REQUIRED)
catkin_simple(ALL_DEPS_REQUIRED)

############
# PROTOBUF #
############
set(PROTO_DEFNS proto/id.proto)
PROTOBUF_CATKIN_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

###########
# LIBRARY #
###########
add_definitions(-std=c++11)
cs_add_library(${PROJECT_NAME} src/condition.cc
                               src/hash-id.cc
                               src/reader-first-reader-writer-lock.cc
                               src/reader-writer-lock.cc
                               src/unique-id.cc
                               ${PROTO_SRCS} 
                               ${PROTO_HDRS})
                               
cs_install()
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR}
          CFG_EXTRAS dmap_common-extras.cmake)
