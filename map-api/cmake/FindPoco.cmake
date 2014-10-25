# Tries to find Poco
# defines
# POCO_FOUND
# POCO_INCLUDE_DIR
# POCO_LIBRARIES

find_path(POCO_INCLUDE_DIR Poco/Poco.h)

find_library(PocoFoundation_LIBRARY PocoFoundation)
find_library(PocoData_LIBRARY PocoData)
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin") 
  find_library(PocoSQLite_LIBRARY PocoDataSQLite)
else()
  find_library(PocoSQLite_LIBRARY PocoSQLite)
endif()

set(POCO_LIBRARIES ${PocoFoundation_LIBRARY}
                   ${PocoData_LIBRARY}
                   ${PocoSQLite_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Poco DEFAULT_MSG
  POCO_INCLUDE_DIR PocoFoundation_LIBRARY PocoData_LIBRARY PocoSQLite_LIBRARY)