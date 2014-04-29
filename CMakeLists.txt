cmake_minimum_required(VERSION 2.8.3)
project(aslam_posegraph)

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/)

find_package(catkin REQUIRED roscpp)
find_package(Eigen REQUIRED)
find_package(GLog REQUIRED)

include(${PROJECT_SOURCE_DIR}/cmake/dependencies.cmake)

catkin_package(
    DEPENDS eigen GLog GFlags
    CATKIN_DEPENDS roscpp kindr
    INCLUDE_DIRS include ${Eigen_INCLUDE_DIRS}
    LIBRARIES ${PROJECT_NAME}
    CFG_EXTRAS export_flags.cmake FindGLog.cmake
)

###########
## Build ##
###########

include_directories(include ${Eigen_INCLUDE_DIRS} ${catkin_INCLUDE_DIRS})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-parameter")
if (CMAKE_COMPILER_IS_GNUCC)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

add_library(${PROJECT_NAME} src/edge.cc src/vertex.cc src/pose-graph.cc src/hash-id.cc)
target_link_libraries(${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})
# add_dependencies(${PROJECT_NAME})

#############
## Testing ##
#############

catkin_add_gtest(test_posegraph test/test_posegraph.cc)
target_link_libraries(test_posegraph ${PROJECT_NAME} ${GLOG_LIBRARY})
