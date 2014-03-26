cmake_minimum_required(VERSION 2.8.3)
project(aslam_map_manager)

find_package(catkin REQUIRED)

include(ExternalProject)

set(VERSION 0.0.0)

set(USING_CATKIN true)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CATKIN_DEVEL_PREFIX}/bin)

add_subdirectory(src)