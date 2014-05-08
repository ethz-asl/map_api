cmake_minimum_required (VERSION 2.8)
project(map_api)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

find_package(catkin_simple REQUIRED)

link_directories(${CATKIN_DEVEL_PREFIX}/lib)

catkin_simple()

############
# PROTOBUF #
############
file (GLOB_RECURSE PROTO_DEFNS proto/*.proto)
PROTOBUF_CATKIN_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic -Wno-unused-local-typedefs)

# Core Library available to all applications
SET(CORE_SOURCE  src/cr-table-interface.cc
                 src/cru-table-interface.cc
                 src/history.cc
                 src/map-api-core.cc
                 src/map-api-hub.cc
                 src/revision.cc
                 src/time.cc
                 src/transaction.cc)
                 
cs_add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_SRCS})

##########
# GTESTS #
##########

catkin_add_gtest(test_table_interface_test test/test_main.cpp
  test/table_interface_test.cpp)
target_link_libraries(test_table_interface_test ${PROJECT_NAME})

catkin_add_gtest(test_core_test test/test_main.cpp test/core_test.cpp)
target_link_libraries(test_core_test ${PROJECT_NAME})

catkin_add_gtest(test_transaction_test test/test_main.cpp 
  test/transaction_test.cpp)
target_link_libraries(test_transaction_test ${PROJECT_NAME})

##########
# EXPORT #
##########

cs_install()
# we need to add the current binary dir to include dirs due to
# protobuf, also exporting sm_common header directory for sm::HashId 
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR} 
                       ${sm_common_INCLUDE_DIRS})