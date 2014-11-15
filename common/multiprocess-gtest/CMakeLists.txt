cmake_minimum_required (VERSION 2.8)
project(multiprocess_gtest)

find_package(catkin_simple REQUIRED)

catkin_simple(ALL_DEPS_REQUIRED)

#############
# LIBRARIES #
#############
add_definitions(--std=c++11)

cs_add_library(${PROJECT_NAME} src/multiprocess-fixture.cc)
target_link_libraries(${PROJECT_NAME})

cs_install()
cs_export(INCLUDE_DIRS include)