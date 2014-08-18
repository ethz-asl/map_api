cmake_minimum_required (VERSION 2.8)
project(map_api)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

find_package(catkin_simple REQUIRED)
find_package(Poco REQUIRED)

catkin_simple()

############
# PROTOBUF #
############
SET(PROTO_DEFNS proto/chord-index.proto 
                proto/core.proto
                proto/chunk.proto
                proto/net-table.proto)
PROTOBUF_CATKIN_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic -Wno-unused-local-typedefs)

# Core Library available to all applications
SET(CORE_SOURCE  src/chord-index.cc
                 src/chunk.cc
                 src/chunk-manager.cc
                 src/chunk-transaction.cc
                 src/core.cc
                 src/cr-table.cc
                 src/cr-table-ram-cache.cc
                 src/cru-table.cc
                 src/cru-table-ram-cache.cc
                 src/file-discovery.cc
                 src/hub.cc
                 src/id.cc
                 src/ipc.cc
                 src/local-transaction.cc
                 src/logical-time.cc
                 src/message.cc
                 src/net-table.cc
                 src/net-table-index.cc
                 src/net-table-manager.cc
                 src/net-table-transaction.cc
                 src/peer.cc
                 src/peer-id.cc
                 src/peer-handler.cc
                 src/revision.cc
                 src/server-discovery.cc
                 src/sqlite-interface.cc
                 src/table-descriptor.cc
                 src/transaction.cc)
                 
cs_add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_SRCS} ${PROTO_HDRS})
# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} zeromq_catkin gflags_catkin glog_catkin)
target_link_libraries(${PROJECT_NAME} ${POCO_LIBRARIES})

cs_add_executable(discovery-server discovery-server/discovery-server.cc
                   ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(discovery-server ${PROJECT_NAME})

##########
# GTESTS #
##########
cs_add_library(${PROJECT_NAME}_test test/map_api_multiprocess_fixture.cpp)

catkin_add_gtest(test_chord_index_test test/chord_index_test.cpp)
target_link_libraries(test_chord_index_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_chunk_test test/chunk_test.cpp)
target_link_libraries(test_chunk_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_table_interface_test test/table_interface_test.cpp)
target_link_libraries(test_table_interface_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_core_test test/core_test.cpp)
target_link_libraries(test_core_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_local_transaction_test test/local_transaction_test.cpp)
target_link_libraries(test_local_transaction_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_net_table_test test/net_table_test.cpp)
target_link_libraries(test_net_table_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

catkin_add_gtest(test_hub_test test/hub_test.cpp)
target_link_libraries(test_hub_test ${PROJECT_NAME} ${PROJECT_NAME}_test)

##########
# EXPORT #
##########

cs_install()
# we need to add the current binary dir to include dirs due to
# protobuf, also exporting sm_common header directory for sm::HashId 
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR} 
                       ${sm_common_INCLUDE_DIRS})