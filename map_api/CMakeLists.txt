cmake_minimum_required (VERSION 2.8)
project(map_api)

include(ExternalProject)

find_package(catkin REQUIRED COMPONENTS)

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

######################
# CATKIN DECLARATION #
######################

catkin_package(
    DEPENDS eigen GLog GFlags Protobuf
    CATKIN_DEPENDS 
    INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR}
    LIBRARIES ${PROJECT_NAME}
    CFG_EXTRAS FindGFlags.cmake FindGLog.cmake
)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/bin)

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic)

include_directories(include ${catkin_INCLUDE_DIRS}
                    ${ZEROMQ_INCLUDE_DIRS} ${POCO_INCLUDE_DIRS}
                    ${Eigen_INCLUDE_DIRS})

# Core Library available to all applications
SET(CORE_SOURCE  src/cr-table-interface.cc
                 src/cru-table-interface.cc
                 src/hash.cc
                 src/history.cc
                 src/map-api-core.cc
                 src/map-api-hub.cc
                 src/revision.cc
                 src/time.cc
                 src/transaction.cc)
                
add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_HDRS} ${PROTO_SRCS})
add_dependencies(${PROJECT_NAME} ${BUILD_DEPEND})
target_link_libraries(${PROJECT_NAME} ${LINK_WITH})

##########
# GTESTS #
##########

catkin_add_gtest(test_table_interface_test test/test_main.cpp
  test/table_interface_test.cpp)
target_link_libraries(test_table_interface_test ${PROJECT_NAME} ${LINK_WITH})

catkin_add_gtest(test_hash_test test/test_main.cpp test/hash_test.cpp)
target_link_libraries(test_hash_test ${PROJECT_NAME} ${LINK_WITH})

catkin_add_gtest(test_core_test test/test_main.cpp test/core_test.cpp)
target_link_libraries(test_core_test ${PROJECT_NAME} ${LINK_WITH})
