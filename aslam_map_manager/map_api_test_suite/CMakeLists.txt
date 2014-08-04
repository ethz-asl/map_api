cmake_minimum_required (VERSION 2.8)
project(map_api_test_suite)

find_package(catkin_simple REQUIRED)

catkin_simple()

#############
# LIBRARIES #
#############
add_definitions(--std=c++0x -Wall -Wextra -pedantic)

SET(CORE_SOURCE  src/multiprocess_fixture.cc)
                 
cs_add_library(${PROJECT_NAME} ${CORE_SOURCE} ${PROTO_SRCS} ${PROTO_HDRS})
# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} gflags_catkin glog_catkin)

cs_install()
cs_export(INCLUDE_DIRS include)