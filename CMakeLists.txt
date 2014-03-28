cmake_minimum_required(VERSION 2.8.3)
project(aslam_posegraph)

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/)

find_package(catkin REQUIRED roscpp)
find_package(Eigen REQUIRED)
find_package(GLog REQUIRED)

include(${PROJECT_SOURCE_DIR}/cmake/dependencies.cmake)

include_directories(include ${Eigen_INCLUDE_DIRS} ${catkin_INCLUDE_DIRS})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-parameter")
if (CMAKE_COMPILER_IS_GNUCC)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

catkin_package(
    DEPENDS eigen GLog GFlags lemon
    CATKIN_DEPENDS roscpp
    INCLUDE_DIRS include ${Eigen_INCLUDE_DIRS}
    LIBRARIES ${PROJECT_NAME}
    CFG_EXTRAS export_flags.cmake FindGLog.cmake
)

include_directories(${LEMON_INCLUDE_DIRS})

add_library(${PROJECT_NAME} src/edge.cc)
target_link_libraries(${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})
add_dependencies(${PROJECT_NAME} lemon)

add_executable(lemon_test src/lemon-test.cc)
target_link_libraries(lemon_test ${catkin_LIBRARIES} ${GLOG_LIBRARY} ${PROJECT_NAME})
add_dependencies(lemon_test lemon)


#catkin_add_gtest(test_ src/test/test_.cc)
#target_link_libraries(test_ pthread ${PROJECT_NAME} ${GLOG_LIBRARY})

