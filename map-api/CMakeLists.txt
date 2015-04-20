cmake_minimum_required (VERSION 2.8)
project(map_api)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

find_package(catkin_simple REQUIRED)

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
SET(CORE_SOURCE  src/internal/trackee-multimap.cc
                 src/cache-base.cc
                 src/chord-index.cc
                 src/chunk.cc
                 src/chunk-data-container-base.cc
                 src/chunk-data-ram-container.cc
                 src/chunk-data-stxxl-container.cc
                 src/chunk-manager.cc
                 src/chunk-transaction.cc
                 src/core.cc
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
                 src/spatial-index-cell-data.cc
                 src/table-descriptor.cc
                 src/transaction.cc
                 src/workspace.cc)
                 
cs_add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_SRCS} ${PROTO_HDRS})
# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} zeromq_catkin gflags_catkin glog_catkin)
target_link_libraries(${PROJECT_NAME} pthread)

cs_add_executable(discovery-server discovery-server/discovery-server.cc
                   ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(discovery-server ${PROJECT_NAME})

##########
# GTESTS #
##########
catkin_add_gtest(test_cache_test test/cache_test.cc)
target_link_libraries(test_cache_test ${PROJECT_NAME})

catkin_add_gtest(test_chord_index_test test/chord_index_test.cc)
target_link_libraries(test_chord_index_test ${PROJECT_NAME})

catkin_add_gtest(test_chunk_test test/chunk_test.cc)
target_link_libraries(test_chunk_test ${PROJECT_NAME})

if(!APPLE)
  catkin_add_gtest(test_discovery test/discovery_test.cc)
  target_link_libraries(test_discovery ${PROJECT_NAME})
endif()

catkin_add_gtest(test_chunk_data_container_test 
  test/chunk_data_container_test.cc)
target_link_libraries(test_chunk_data_container_test ${PROJECT_NAME})

catkin_add_gtest(test_core_test test/core_test.cc)
target_link_libraries(test_core_test ${PROJECT_NAME})

catkin_add_gtest(test_net_table_test test/net_table_test.cc)
target_link_libraries(test_net_table_test ${PROJECT_NAME})

catkin_add_gtest(test_spatial_index_test test/spatial_index_test.cc)
target_link_libraries(test_spatial_index_test ${PROJECT_NAME})

catkin_add_gtest(test_transaction_test test/transaction-test.cc)
target_link_libraries(test_transaction_test ${PROJECT_NAME})

catkin_add_gtest(test_hub_test test/hub_test.cc)
target_link_libraries(test_hub_test ${PROJECT_NAME})

catkin_add_gtest(test_proto_table_file_io_test test/proto_table_file_io_test.cc)
target_link_libraries(test_proto_table_file_io_test ${PROJECT_NAME})

catkin_add_gtest(test_proto_stl_stream_test test/proto_stl_stream_test.cc)
target_link_libraries(test_proto_stl_stream_test ${PROJECT_NAME})

catkin_add_gtest(test_reader_writer_lock_test test/reader_writer_lock_test.cc)
target_link_libraries(test_reader_writer_lock_test ${PROJECT_NAME})

##########
# EXPORT #
##########

cs_install()
# we need to add the current binary dir to include dirs due to
# protobuf, also exporting sm_common header directory for sm::HashId 
cs_export(INCLUDE_DIRS include ${CMAKE_CURRENT_BINARY_DIR} 
                       ${sm_common_INCLUDE_DIRS})
