cmake_minimum_required(VERSION 3.5.0 FATAL_ERROR) 

find_package(IRODS 4.2.6 EXACT REQUIRED)

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)
set (CMAKE_CXX_STANDARD 14)

project(beegfs-irods-connector C CXX)

find_package(Threads REQUIRED)

include(${IRODS_TARGETS_PATH})

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

if (NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/" CACHE STRING "Install root path. Change from \"/\" for run-in-place installations." FORCE)
  message(STATUS "Setting unspecified CMAKE_INSTALL_PREFIX to '${CMAKE_INSTALL_PREFIX}'. This is the correct setting for normal builds.")
endif()

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib)


link_directories(${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib /usr/local/lib)

message("IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME is ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}")
message("CMAKE_INSTALL_RPATH is ${CMAKE_INSTALL_RPATH}")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++")
add_compile_options(-nostdinc++ -Wall -Wextra -Werror -Wno-unused-parameter -Wno-sign-compare)


link_libraries(c++abi
    pthread
    dl
    rt
    irods_client
    irods_common
    irods_plugin_dependencies
    capnp
    kj
    zmq
    sqlite3
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so
    ${IRODS_EXTERNALS_FULLPATH_ZMQ}/lib/libzmq.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so)


include_directories(/usr/include/irods
    ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1 
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${CMAKE_SOURCE_DIR}/src/
    /opt/irods-externals/cppzmq4.1-0/include
    /opt/irods-externals/jansson2.7-0/include
    /home/jjames/v7/event_listener/include)

set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,-z,defs")

add_executable(beegfs_irods_connector ${PROJECT_SOURCE_DIR}/src/change_table.capnp.c++ ${PROJECT_SOURCE_DIR}/src/beegfs_change_table.cpp ${PROJECT_SOURCE_DIR}/src/irods_ops.cpp ${PROJECT_SOURCE_DIR}/src/config.cpp ${PROJECT_SOURCE_DIR}/src/main.cpp)

#target_link_libraries(
    #beegfs_irods_connector
  #PRIVATE
  #${IRODS_API_PLUGIN_LINK_LIBRARIES_${PLUGIN}}
  #${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
  #${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
  #/usr/local/lib/libcapnp.so
  #/usr/local/lib/libkj.so
  #/usr/local/lib/libzmq.so
  #  )

set_target_properties(beegfs_irods_connector PROPERTIES LINKER_LANGUAGE CXX)

add_custom_command(TARGET beegfs_irods_connector PRE_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_SOURCE_DIR}/src/beegfs_irods_connector_config.json ${CMAKE_CURRENT_BINARY_DIR}/beegfs_irods_connector_config.json)

#add_custom_command(TARGET beegfs_irods_connector PRE_BUILD
#    COMMAND capnp compile -oc++ ${PROJECT_SOURCE_DIR}/src/change_table.capnp)

add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/src/change_table.capnp.c++  ${PROJECT_SOURCE_DIR}/src/change_table.capnp.h
    COMMAND capnp compile -oc++:${PROJECT_SOURCE_DIR}/src --src-prefix=${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/src/change_table.capnp)

#add_dependencies(beegfs_irods_connector ${PROJECT_SOURCE_DIR}/src/change_table.capnp.c++ ${PROJECT_SOURCE_DIR}/src/change_table.capnp.h)

