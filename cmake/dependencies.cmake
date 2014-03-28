cmake_minimum_required(VERSION 2.8.3)
include(ExternalProject)

ExternalProject_Add(lemon
  URL http://lemon.cs.elte.hu/pub/sources/lemon-1.3.tar.gz
  CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}  ../lemon
  INSTALL_DIR ${CMAKE_BINARY_DIR}
  BUILD_COMMAND make -j8 -l8
  INSTALL_COMMAND make install
)
message(CMAKE_BINARY_DIR ${CMAKE_BINARY_DIR})
set(LEMON_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/include/)
