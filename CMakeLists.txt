cmake_minimum_required(VERSION 2.8.3)
project(ceres_error_terms)

find_package(catkin_simple REQUIRED)
catkin_simple()

include_directories(${Eigen_INCLUDE_DIRS})
add_definitions(${EIGEN_DEFINITIONS})

add_definitions(--std=c++11 -Wno-enum-compare)

cs_add_library(${PROJECT_NAME} src/quaternion_param_hamilton.cc)

catkin_add_gtest(test_relaxation
	test/test_posegraph_relaxation_test.cpp)
target_link_libraries(test_relaxation ${PROJECT_NAME})
	
catkin_add_gtest(test_covariance
	test/test_posegraph_covariance_computation_test.cpp)
target_link_libraries(test_covariance ${PROJECT_NAME})

catkin_add_gtest(test_quat_parametrization
	test/test_quaternion_parametrization_test.cpp)
target_link_libraries(test_quat_parametrization ${PROJECT_NAME})

catkin_add_gtest(test_few_points_quaternion_test
	test/test_few_points_quaternion_test.cpp)
target_link_libraries(test_few_points_quaternion_test ${PROJECT_NAME})
	
catkin_add_gtest(test_switchable_constraints_test
	test/test_switchable_constraints_test.cpp)
target_link_libraries(test_switchable_constraints_test ${PROJECT_NAME})




