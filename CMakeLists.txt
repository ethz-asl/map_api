cmake_minimum_required(VERSION 2.8.3)
project(ceres_error_terms)

# Set the build type.  Options are:
#  Coverage       : w/ debug symbols, w/o optimization, w/ code-coverage
#  Debug          : w/ debug symbols, w/o optimization
#  Release        : w/o debug symbols, w/ optimization
#  RelWithDebInfo : w/ debug symbols, w/ optimization
#  MinSizeRel     : w/o debug symbols, w/ optimization, stripped binaries
set(CMAKE_BUILD_TYPE Release)

find_package(catkin REQUIRED COMPONENTS kindr ceres)
find_package(Eigen REQUIRED)

add_definitions(--std=c++0x)

catkin_package(
  DEPENDS eigen
  CATKIN_DEPENDS ceres kindr
  INCLUDE_DIRS include ${catkin_INCLUDE_DIRS}
  LIBRARIES
)

###########
## Build ##
###########

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
  ${Eigen_INCLUDE_DIRS}
)
add_definitions(${EIGEN_DEFINITIONS})

#add_library(${PROJECT_NAME})
#add_dependencies(${PROJECT_NAME} ceres)
#target_link_libraries(${PROJECT_NAME}
#  ${catkin_LIBRARIES}
#)

#############
## Testing ##
#############

catkin_add_gtest(test_relaxation test/PosegraphRelaxationTest.cpp)

# currently the library is header-only
target_link_libraries(test_relaxation ${catkin_LIBRARIES})
