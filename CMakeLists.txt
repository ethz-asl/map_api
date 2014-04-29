cmake_minimum_required(VERSION 2.8.3)
project(aslam_posegraph)

find_package(catkin_simple REQUIRED)
catkin_simple()

include_directories(${Eigen_INCLUDE_DIRS})

#common commands for building c++ executables and libraries
cs_add_library(${PROJECT_NAME} 
  src/Vertex.cpp
  src/Edge.cpp
  src/PoseGraph.cpp
  )

# Avoid clash with tr1::tuple: https://code.google.com/p/googletest/source/browse/trunk/README?r=589#257
add_definitions(-DGTEST_USE_OWN_TR1_TUPLE=0)

catkin_add_gtest(${PROJECT_NAME}_tests
  test/test_main.cpp
  test/PoseGraphTests.cpp
)

target_link_libraries(${PROJECT_NAME}_tests ${PROJECT_NAME})

#INCLUDE(src/autogen_files.cmake)
add_python_export_library(${PROJECT_NAME}_python src/${PROJECT_NAME}
  src/module.cpp
  src/python/Vertex.cpp
  src/python/Edge.cpp
  src/python/PoseGraph.cpp
)
target_link_libraries(${PROJECT_NAME}_python ${PROJECT_NAME})

cs_install()
cs_export()
