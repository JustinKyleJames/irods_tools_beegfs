cmake_minimum_required(VERSION 3.5.0 FATAL_ERROR) #CPACK_DEBIAN_<COMPONENT>_PACKAGE_NAME

project(irods-beegfs-api C CXX)

find_package(IRODS 4.2.6 EXACT REQUIRED)

if (NOT ODBC_LIBRARY)
  find_library(ODBC_LIBRARY odbc)
  if (ODBC_LIBRARY)
    message(STATUS "Found odbc: ${ODBC_LIBRARY}")
  else()
    message(FATAL_ERROR "odbc library not found")
  endif()
endif()

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)

include(${IRODS_TARGETS_PATH})

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

if (NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/" CACHE STRING "Install root path. Change from \"/\" for run-in-place installations." FORCE)
  message(STATUS "Setting unspecified CMAKE_INSTALL_PREFIX to '${CMAKE_INSTALL_PREFIX}'. This is the correct setting for normal builds.")
endif()

if (NOT CPACK_PACKAGING_INSTALL_PREFIX)
  set(CPACK_PACKAGING_INSTALL_PREFIX "/" CACHE STRING "Package root path. \"/\" is correct for normal package builds.." FORCE)
  message(STATUS "Setting unspecified CPACK_PACKAGING_INSTALL_PREFIX to '${CPACK_PACKAGING_INSTALL_PREFIX}'. This is the correct setting for normal builds.")
endif()

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++")
add_compile_options(-nostdinc++ -Wall -Wextra -Werror -Wno-unused-parameter)
link_libraries(c++abi)
include_directories(${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1)
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,-z,defs")

#include(icommand.cmake)

set(
  IRODS_DATABASE_PLUGINS ""
  )

if (EXISTS "/usr/lib/irods/plugins/database/libpostgres.so")
  list(APPEND IRODS_DATABASE_PLUGINS postgres)
endif()

if (EXISTS "/usr/lib/irods/plugins/database/libmysql.so")
  list(APPEND IRODS_DATABASE_PLUGINS mysql)
endif()

if (EXISTS "/usr/lib/irods/plugins/database/liboracle.so")
  list(APPEND IRODS_DATABASE_PLUGINS oracle)
endif()

if (EXISTS "/usr/lib/irods/plugins/database/libcockroachdb.so")
  list(APPEND IRODS_DATABASE_PLUGINS cockroachdb)
endif()

if (NOT IRODS_DATABASE_PLUGINS)
    message(FATAL_ERROR "No database plugin encountered.  You must install an irods database plugin to build the connector.  Please install one of the following packages.\nirods-database-plugin-postgres\nirods-database-plugin-mysql\nirods-database-plugin-oracle\nor build and install the cockroachdb plugin.")
endif()


#set(
#  IRODS_DATABASE_PLUGINS
#  postgres
#  mysql
#  oracle
#  )

set(CPACK_COMPONENTS_ALL ${IRODS_DATABASE_PLUGINS})

foreach (DB_TYPE ${IRODS_DATABASE_PLUGINS})

include(api_plugin.cmake)
#set(COMPONENT_NAME ${DB_TYPE})


#set(CPACK_RPM_COMPONENT_INSTALL ON)

set(CPACK_COMPONENT_${DB_TYPE}_PACKAGE_FILE_NAME "irods-beegfs-api-plugin-${DB_TYPE}${IRODS_PACKAGE_FILE_NAME_SUFFIX}")

message(STATUS "CPACK_PACKAGE_FILE_NAME=${CPACK_PACKAGE_FILE_NAME}")

set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENTS_GROUPING IGNORE)
set(CPACK_PACKAGE_VERSION ${IRODS_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${IRODS_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${IRODS_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${IRODS_VERSION_PATCH})
set(CPACK_PACKAGE_CONTACT "Renaissance Computing Institute <info@irods.org>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The integrated Rule-Oriented Data System")
set(CPACK_PACKAGE_VENDOR "Renaissance Computing Institute <info@irods.org>")

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_SECTION "contrib/science")
set(CPACK_DEBIAN_COMPRESSION_TYPE "gzip")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "extra")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://irods.org")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)

set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_RELEASE "1")
set(CPACK_RPM_PACKAGE_LICENSE "BSD-3-Clause")
set(CPACK_RPM_PACKAGE_VENDOR "iRODS Consortium")
set(CPACK_RPM_PACKAGE_URL "https://irods.org")
set(CPACK_RPM_PACKAGE_AUTOREQ 0)
set(CPACK_RPM_PACKAGE_AUTOPROV 0)

set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)

set(CPACK_COMPONENT_${DB_TYPE}_DEBIAN_PACKAGE_NAME "irods-beegfs-api-plugin-${DB_TYPE}")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "${IRODS_PACKAGE_DEPENDENCIES_STRING}, irods-runtime (= ${IRODS_VERSION}), libc6")

set(CPACK_COMPONENT_${DB_TYPE}_RPM_PACKAGE_NAME "irods-beegfs-api-plugin-${DB_TYPE}")
if (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "centos" OR IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "centos linux")
  set(CPACK_RPM_PACKAGE_REQUIRES "${IRODS_PACKAGE_DEPENDENCIES_STRING}, irods-runtime = ${IRODS_VERSION}, openssl")
elseif (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "opensuse")
  set(CPACK_RPM_PACKAGE_REQUIRES "${IRODS_PACKAGE_DEPENDENCIES_STRING}, irods-runtime = ${IRODS_VERSION}, libopenssl1_0_0")
endif()

message(STATUS "CPACK_GENERATOR ${CPACK_GENERATOR}")

if (NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR ${IRODS_CPACK_GENERATOR} CACHE STRING "CPack generator to use, e.g. {DEB, RPM, TGZ}." FORCE)
  message(STATUS "Setting unspecified CPACK_GENERATOR to ${CPACK_GENERATOR}. This is the correct setting for normal builds.")
endif()

endforeach()

add_custom_command(OUTPUT ${CMAKE_SOURCE_DIR}/../beegfs_irods_connector/src/change_table.capnp.h 
    COMMAND capnp compile -oc++:${CMAKE_SOURCE_DIR}/../beegfs_irods_connector/src --src-prefix=${CMAKE_SOURCE_DIR}/../beegfs_irods_connector/src ${CMAKE_SOURCE_DIR}/../beegfs_irods_connector/src/change_table.capnp)

include(CPack) 
