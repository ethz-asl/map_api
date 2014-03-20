cmake_minimum_required(VERSION 2.8.3)
project(aslam_map_manager)

find_package(catkin REQUIRED)

include(ExternalProject)

set(VERSION 0.0.0)

# TODO(tcies) resolve dependencies here already? If so, how?

ExternalProject_Add(aslam_map_manager
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src
  INSTALL_COMMAND ""
)

