cmake_minimum_required (VERSION 2.8)
project(map_api_benchmarks)

find_package(catkin_simple REQUIRED)

catkin_simple()

#############
# LIBRARIES #
#############
add_definitions(--std=c++11)

cs_add_library(${PROJECT_NAME} src/app.cc
                               src/kmeans-view.cc
                               src/kmeans-subdivision-hoarder.cc
                               src/kmeans-subdivision-view.cc
                               src/kmeans-subdivision-worker.cc
                               src/multi-kmeans-hoarder.cc
                               src/multi-kmeans-worker.cc)
                               
# tool for plotting lock sequence diagram
cs_add_executable(lock-sequence-reader src/lock-sequence-reader.cc)

# we need to add dependencies of external project packages to fetch headers
# before compilation
add_dependencies(${PROJECT_NAME} gflags_catkin glog_catkin)

catkin_add_gtest(test_kmeans test/test_kmeans.cc)
target_link_libraries(test_kmeans ${PROJECT_NAME})

cs_install()
cs_export()