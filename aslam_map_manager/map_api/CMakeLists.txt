cmake_minimum_required (VERSION 2.8)
project(map_api)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

find_package(catkin_simple REQUIRED)
find_package(Poco REQUIRED)

catkin_simple(ALL_DEPS_REQUIRED)

############
# PROTOBUF #
############
add_definitions(--std=c++11)

SET(PROTO_DEFNS proto/chord-index.proto 
                proto/core.proto
                proto/chunk.proto
                proto/ipc.proto
                proto/net-table.proto)
PROTOBUF_CATKIN_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_DEFNS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#############
# LIBRARIES #
#############

# Core Library available to all applications
SET(CORE_SOURCE  src/cache-base.cc
                 src/chord-index.cc
                 src/chunk.cc
                 src/chunk-manager.cc
                 src/chunk-transaction.cc
                 src/core.cc
                 src/cr-table.cc
                 src/cr-table-ram-map.cc
                 src/cr-table-stxxl-map.cc
                 src/cru-table.cc
                 src/cru-table-ram-map.cc
                 src/cru-table-stxxl-map.cc
                 src/file-discovery.cc
                 src/hub.cc
                 src/ipc.cc
                 src/logical-time.cc
                 src/message.cc
                 src/net-table.cc
                 src/net-table-index.cc
                 src/net-table-manager.cc
                 src/net-table-transaction.cc
                 src/peer.cc
                 src/peer-id.cc
                 src/peer-handler.cc
                 src/proto-table-file-io.cc
                 src/revision.cc
                 src/server-discovery.cc
                 src/spatial-index.cc
                 src/table-descriptor.cc
                 src/transaction.cc
                 src/unique-id.cc
                 src/visual-frame-resource-loader.cc)
                 
cs_add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_SRCS} ${PROTO_HDRS})
# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} zeromq_catkin gflags_catkin glog_catkin)
target_link_libraries(${PROJECT_NAME} ${POCO_LIBRARIES})

cs_add_executable(discovery-server discovery-server/discovery-server.cc
                   ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(discovery-server ${PROJECT_NAME})

# TODO(tcies) reconsider for radix tree hash map benchmarking
# catkin_add_gtest(test_hash_id_hashing
#   test/test_hash_id_hashing.cc)
# target_link_libraries(test_hash_id_hashing ${PROJECT_NAME} pthread)

##########
# GTESTS #
##########
file(COPY test/test-data DESTINATION 
       ${CATKIN_DEVEL_PREFIX}/${CATKIN_PACKAGE_BIN_DESTINATION})

cs_add_library(${PROJECT_NAME}_fixture STATIC test/map_api_fixture.cc)
target_link_libraries(${PROJECT_NAME}_fixture ${PROJECT_NAME} gtest pthread)

cs_add_library(net_table_fixture STATIC test/net_table_fixture.cc)
target_link_libraries(net_table_fixture ${PROJECT_NAME}_fixture)


catkin_add_gtest(test_cache_test test/cache_test.cc)
target_link_libraries(test_cache_test net_table_fixture)

catkin_add_gtest(test_chord_index_test test/chord_index_test.cc)
target_link_libraries(test_chord_index_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_chunk_test test/chunk_test.cc)
target_link_libraries(test_chunk_test net_table_fixture)

catkin_add_gtest(test_discovery test/discovery_test.cc)
target_link_libraries(test_discovery ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_table_interface_test test/table_interface_test.cc)
target_link_libraries(test_table_interface_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_core_test test/core_test.cc)
target_link_libraries(test_core_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_net_table_test test/net_table_test.cc)
target_link_libraries(test_net_table_test net_table_fixture)

catkin_add_gtest(test_spatial_index_test test/spatial_index_test.cc)
target_link_libraries(test_spatial_index_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_transaction_test test/transaction-test.cc)
target_link_libraries(test_transaction_test net_table_fixture)

catkin_add_gtest(test_hub_test test/hub_test.cc)
target_link_libraries(test_hub_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_proto_table_file_io_test test/proto_table_file_io_test.cc)
target_link_libraries(test_proto_table_file_io_test net_table_fixture)

catkin_add_gtest(test_proto_stl_stream_test test/proto_stl_stream_test.cc)
target_link_libraries(test_proto_stl_stream_test ${PROJECT_NAME}_fixture)

catkin_add_gtest(test_visual_frame_resource_loader_test 
                 test/visual-frame-resource-loader-test.cc
                 WORKING_DIRECTORY
                 ${CATKIN_DEVEL_PREFIX}/${CATKIN_PACKAGE_BIN_DESTINATION})
target_link_libraries(test_visual_frame_resource_loader_test ${PROJECT_NAME}_fixture)

##########
# EXPORT #
##########

cs_install()
# we need to add the current binary dir to include dirs due to
# protobuf, also exporting sm_common header directory for sm::HashId 
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR} 
                       ${sm_common_INCLUDE_DIRS})
