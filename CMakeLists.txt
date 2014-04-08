cmake_minimum_required(VERSION 2.8.3)
project(ceres_error_terms)

find_package(catkin REQUIRED COMPONENTS
  ceres
  roscpp
  eigen REQUIRED
)

catkin_package(
  CATKIN_DEPENDS ceres
  DEPENDS eigen
)

###########
## Build ##
###########

include_directories(
  ${catkin_INCLUDE_DIRS}
  ${EIGEN_INCLUDE_DIRS}
)
add_definitions(${EIGEN_DEFINITIONS})

add_executable(6DoFposegraph src/6DoFposegraph.cpp)
target_link_libraries(6DoFposegraph
  ${catkin_LIBRARIES}
)

add_definitions(--std=c++0x)


#############
## Testing ##
#############


