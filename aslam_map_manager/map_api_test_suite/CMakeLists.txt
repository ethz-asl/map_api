cmake_minimum_required (VERSION 2.8)
project(map_api_test_suite)

find_package(catkin_simple REQUIRED)

catkin_simple(ALL_DEPS_REQUIRED)

#############
# LIBRARIES #
#############
add_definitions(--std=c++11)

include_directories("/usr/include/")

cs_add_library(${PROJECT_NAME} src/multiprocess_fixture.cc)

# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} gflags_catkin glog_catkin)

target_link_libraries(${PROJECT_NAME} gtest)

cs_install()
cs_export(INCLUDE_DIRS include)
