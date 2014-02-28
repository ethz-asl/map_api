cmake_minimum_required(VERSION 2.8.3)
project(ceres_error_terms)

find_package(catkin REQUIRED COMPONENTS
  ceres
  roscpp
)

catkin_package(
)

###########
## Build ##
###########

include_directories(
  ${catkin_INCLUDE_DIRS}
)

add_executable(6DoFposegraph src/6DoFposegraph.cpp)
target_link_libraries(6DoFposegraph
  ${catkin_LIBRARIES}
)

add_definitions(--std=c++0x)

#############
## Testing ##
#############


