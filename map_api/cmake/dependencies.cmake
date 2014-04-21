# Fetch POCO
ExternalProject_Add(POCO
  URL http://pocoproject.org/releases/poco-1.4.6/poco-1.4.6p2-all.tar.gz
  # omitting ODBC in order to not have to install additional dependencies
  CONFIGURE_COMMAND ../POCO/configure --no-tests --no-samples --prefix=${CMAKE_BINARY_DIR}/pocolib --omit=Data/ODBC,Net,Zip
)

set(POCO_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/pocolib/include)
set(POCO_FOUNDATION ${CMAKE_BINARY_DIR}/pocolib/lib/libPocoFoundation.so)
set(POCO_DATA ${CMAKE_BINARY_DIR}/pocolib/lib/libPocoData.so)
set(POCO_SQLITE ${CMAKE_BINARY_DIR}/pocolib/lib/libPocoDataSQLite.so)
set(POCO_LIBRARIES ${POCO_FOUNDATION} ${POCO_DATA} ${POCO_SQLITE})
LIST(APPEND LINK_WITH ${POCO_LIBRARIES})
LIST(APPEND BUILD_DEPEND POCO)

# Fetch ZeroMQ.
ExternalProject_Add(ZMQ
  URL http://download.zeromq.org/zeromq-4.0.3.tar.gz
  URL_MD5 8348341a0ea577ff311630da0d624d45
  CONFIGURE_COMMAND ../ZMQ/configure --includedir=${CMAKE_BINARY_DIR}/include/ --with-pic --libdir=${CMAKE_BINARY_DIR}/lib --bindir=${CMAKE_BINARY_DIR}/bin	
)
# TODO(tcies) replace git by download in order to avoid fetch?
ExternalProject_Add(ZMQ_CPP
  # GIT_REPOSITORY https://github.com/zeromq/cppzmq.git
  PATCH_COMMAND cp ../ZMQ_CPP/zmq.hpp ${CMAKE_BINARY_DIR}/include/zmq.hpp
  DEPENDS ZMQ
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
)
SET(ZEROMQ_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/include)
SET(ZEROMQ_LIBRARIES ${CMAKE_BINARY_DIR}/lib/libzmq.a)
LIST(APPEND LINK_WITH ${ZEROMQ_LIBRARIES})
LIST(APPEND BUILD_DEPEND ZMQ ZMQ_CPP)
