cmake_minimum_required(VERSION 2.8.3)
project(ceres_error_terms)

# Set the build type.  Options are:
#  Coverage       : w/ debug symbols, w/o optimization, w/ code-coverage
#  Debug          : w/ debug symbols, w/o optimization
#  Release        : w/o debug symbols, w/ optimization
#  RelWithDebInfo : w/ debug symbols, w/ optimization
#  MinSizeRel     : w/o debug symbols, w/ optimization, stripped binaries
set(CMAKE_BUILD_TYPE Release)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake/)

find_package(catkin REQUIRED COMPONENTS kindr ceres)
find_package(Eigen REQUIRED)
find_package(GLog REQUIRED)

add_definitions(--std=c++11)

catkin_package(
  DEPENDS eigen GLog
  CATKIN_DEPENDS ceres kindr
  INCLUDE_DIRS include ${catkin_INCLUDE_DIRS}
  LIBRARIES
  CFG_EXTRAS FindGLog.cmake
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

add_library(${PROJECT_NAME} src/quat_param_hamilton.cc)
add_dependencies(${PROJECT_NAME} ceres)
target_link_libraries(${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})

#############
## Testing ##
#############

catkin_add_gtest(test_relaxation
	test/test_posegraph_relaxation_test.cpp)
target_link_libraries(test_relaxation ${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})
	
catkin_add_gtest(test_covariance
	test/test_posegraph_covariance_computation_test.cpp)
target_link_libraries(test_covariance ${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})

catkin_add_gtest(test_quat_parametrization
	test/test_quaternion_parametrization_test.cpp)
target_link_libraries(test_quat_parametrization ${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})

catkin_add_gtest(test_few_points_quaternion_test
	test/test_few_points_quaternion_test.cpp)
target_link_libraries(test_few_points_quaternion_test ${PROJECT_NAME} ${catkin_LIBRARIES} ${GLOG_LIBRARY})




