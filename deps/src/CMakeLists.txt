cmake_minimum_required(VERSION 3.18)
project(modern-cpp-kafka-julia LANGUAGES CXX)

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type (Release/Debug/RelWithDebInfo/MinSizeRel)" FORCE)
endif()

option(STATIC_DEPENDENCY_LINKING "Link external dependencies statically to avoid runtime shared library requirements" OFF)

get_filename_component(PROJECT_ROOT "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
get_filename_component(DEPS_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)

if(NOT DEFINED JULIA_BIN OR JULIA_BIN STREQUAL "")
    if(DEFINED ENV{JULIA} AND NOT "$ENV{JULIA}" STREQUAL "")
        set(JULIA_BIN $ENV{JULIA})
    else()
        find_program(JULIA_BIN julia)
    endif()
endif()

if(NOT JULIA_BIN)
    message(FATAL_ERROR "Could not locate a Julia executable. Set JULIA_BIN or the JULIA environment variable.")
endif()

set(Julia_EXECUTABLE "${JULIA_BIN}" CACHE FILEPATH "Julia executable used to query dependencies")
set(JULIA_PROJECT "${PROJECT_ROOT}")

execute_process(
    COMMAND ${JULIA_BIN} "--project=${JULIA_PROJECT}" -e "using CxxWrap; print(CxxWrap.CxxWrapCore.prefix_path())"
    OUTPUT_VARIABLE CXXWRAP_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_VARIABLE CXXWRAP_PREFIX_ERROR
    RESULT_VARIABLE CXXWRAP_PREFIX_RESULT
)

if(NOT CXXWRAP_PREFIX_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to query CxxWrap prefix using ${JULIA_BIN}: ${CXXWRAP_PREFIX_ERROR}")
endif()

if(NOT EXISTS "${CXXWRAP_PREFIX}")
    message(FATAL_ERROR "CxxWrap prefix path '${CXXWRAP_PREFIX}' does not exist")
endif()

list(APPEND CMAKE_PREFIX_PATH "${CXXWRAP_PREFIX}")
message(STATUS "Using CxxWrap prefix ${CXXWRAP_PREFIX}")

find_package(Julia QUIET)
if(NOT Julia_FOUND)
    message(STATUS "Falling back to Julia autodiscovery via ${JULIA_BIN}")
    set(_julia_info_script "
using Libdl
include_dir = normpath(joinpath(Sys.BINDIR, \"..\", \"include\", \"julia\"))
lib_dir = normpath(joinpath(Sys.BINDIR, \"..\", \"lib\"))
function find_libjulia(lib_dir)
    candidate = normpath(joinpath(lib_dir, \"libjulia.\" * Libdl.dlext))
    if isfile(candidate)
        return candidate
    end
    handle = Libdl.dlopen(\"libjulia\")
    try
        return Libdl.dlpath(handle)
    finally
        Libdl.dlclose(handle)
    end
end
lib_path = find_libjulia(lib_dir)
print(include_dir * \"\\n\" * lib_dir * \"\\n\" * lib_path * \"\\n\" * string(VERSION))
")
    execute_process(
        COMMAND ${JULIA_BIN} "--project=${JULIA_PROJECT}" -e "${_julia_info_script}"
        OUTPUT_VARIABLE _julia_info_raw
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_VARIABLE _julia_info_error
        RESULT_VARIABLE _julia_info_result
    )
    if(NOT _julia_info_result EQUAL 0)
        message(FATAL_ERROR "Failed to query Julia layout: ${_julia_info_error}")
    endif()
    string(REPLACE "\r\n" "\n" _julia_info "${_julia_info_raw}")
    string(REPLACE "\r" "\n" _julia_info "${_julia_info}")
    string(REPLACE "\n" ";" _julia_info_list "${_julia_info}")
    list(LENGTH _julia_info_list _julia_info_len)
    if(_julia_info_len LESS 3)
        message(FATAL_ERROR "Unexpected Julia info output: '${_julia_info_raw}'")
    endif()
    list(GET _julia_info_list 0 Julia_INCLUDE_DIRS)
    list(GET _julia_info_list 1 Julia_LIBRARY_DIR)
    list(GET _julia_info_list 2 Julia_LIBRARY)
    if(_julia_info_len GREATER 3)
        list(GET _julia_info_list 3 Julia_VERSION_STRING)
    endif()
    if(NOT EXISTS "${Julia_INCLUDE_DIRS}")
        message(FATAL_ERROR "Julia include directory not found: ${Julia_INCLUDE_DIRS}")
    endif()
    if(NOT EXISTS "${Julia_LIBRARY}")
        message(FATAL_ERROR "Julia library not found: ${Julia_LIBRARY}")
    endif()
    set(Julia_FOUND TRUE)
    if(NOT TARGET Julia::Julia)
        add_library(Julia::Julia SHARED IMPORTED)
        set_target_properties(Julia::Julia PROPERTIES
            IMPORTED_LOCATION "${Julia_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Julia_INCLUDE_DIRS}"
        )
    endif()
    message(STATUS "Detected Julia headers at ${Julia_INCLUDE_DIRS}")
    message(STATUS "Detected Julia library at ${Julia_LIBRARY}")
endif()

if(Julia_FOUND AND NOT Julia_INCLUDE_DIRS AND DEFINED Julia_INCLUDE_DIR)
    set(Julia_INCLUDE_DIRS "${Julia_INCLUDE_DIR}")
endif()

if(Julia_FOUND AND NOT Julia_LIBRARY_DIR AND DEFINED Julia_LIBRARY)
    get_filename_component(Julia_LIBRARY_DIR "${Julia_LIBRARY}" DIRECTORY)
endif()

if(Julia_FOUND AND NOT TARGET Julia::Julia AND DEFINED Julia_LIBRARY)
    add_library(Julia::Julia SHARED IMPORTED)
    set_target_properties(Julia::Julia PROPERTIES
        IMPORTED_LOCATION "${Julia_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${Julia_INCLUDE_DIRS}"
    )
endif()

find_package(JlCxx REQUIRED CONFIG)
find_package(Threads REQUIRED)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PKG_RDKAFKA QUIET rdkafka)
    pkg_check_modules(PKG_SASL QUIET libsasl2)
endif()

set(_original_find_library_suffixes ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(_original_link_search_start_static ${CMAKE_LINK_SEARCH_START_STATIC})
if(STATIC_DEPENDENCY_LINKING)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".lib")
    set(CMAKE_LINK_SEARCH_START_STATIC ON)
endif()

set(RDKAFKA_ROOT "" CACHE PATH "Root directory of the librdkafka installation")

set(_rdkafka_include_hints)
foreach(_hint ${RDKAFKA_ROOT} $ENV{RDKAFKA_ROOT} ${RDKAFKA_INCLUDEDIR} ${PKG_RDKAFKA_INCLUDE_DIRS})
    if(_hint)
        list(APPEND _rdkafka_include_hints "${_hint}")
    endif()
endforeach()
list(REMOVE_DUPLICATES _rdkafka_include_hints)

set(_rdkafka_library_hints)
foreach(_hint ${RDKAFKA_ROOT} $ENV{RDKAFKA_ROOT} ${RDKAFKA_LIBDIR} ${PKG_RDKAFKA_LIBRARY_DIRS})
    if(_hint)
        list(APPEND _rdkafka_library_hints "${_hint}")
    endif()
endforeach()
list(REMOVE_DUPLICATES _rdkafka_library_hints)

find_path(RDKAFKA_INCLUDE_DIR
    NAMES rdkafka.h
    HINTS ${_rdkafka_include_hints}
    PATH_SUFFIXES include include/librdkafka
    PATHS
        /usr/local/include
        /usr/include
        /opt/homebrew/include
        /opt/local/include
        /usr/local/opt/librdkafka/include
)

find_library(RDKAFKA_LIBRARY
    NAMES rdkafka
    HINTS ${_rdkafka_library_hints}
    PATH_SUFFIXES lib lib64 lib/x86_64-linux-gnu lib/aarch64-linux-gnu lib/arm64
    PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib64
        /opt/homebrew/lib
        /opt/local/lib
        /usr/local/opt/librdkafka/lib
)

if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
    message(STATUS "Attempting to locate librdkafka via Julia's librdkafka_jll artifact")
    set(_rdkafka_jll_script "
import Pkg
try
    using librdkafka_jll
catch err
    Pkg.instantiate()
    using librdkafka_jll
end
lib_path = librdkafka_jll.librdkafka_path
lib_dir = dirname(lib_path)
artifact_root = dirname(lib_dir)
include_dir = normpath(joinpath(artifact_root, \"include\"))
print(include_dir * \"\\n\" * lib_dir * \"\\n\" * lib_path)
")
    execute_process(
        COMMAND ${JULIA_BIN} "--project=${JULIA_PROJECT}" -e "${_rdkafka_jll_script}"
        OUTPUT_VARIABLE _rdkafka_jll_raw
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_VARIABLE _rdkafka_jll_error
        RESULT_VARIABLE _rdkafka_jll_result
    )
    if(_rdkafka_jll_result EQUAL 0)
        string(REPLACE "\r\n" "\n" _rdkafka_jll "${_rdkafka_jll_raw}")
        string(REPLACE "\r" "\n" _rdkafka_jll "${_rdkafka_jll}")
        string(REPLACE "\n" ";" _rdkafka_jll_list "${_rdkafka_jll}")
        list(LENGTH _rdkafka_jll_list _rdkafka_jll_len)
        if(_rdkafka_jll_len GREATER_EQUAL 3)
            list(GET _rdkafka_jll_list 0 RDKAFKA_INCLUDE_DIR)
            list(GET _rdkafka_jll_list 1 RDKAFKA_LIBRARY_DIR)
            list(GET _rdkafka_jll_list 2 RDKAFKA_LIBRARY)
            set(RDKAFKA_INCLUDE_DIR "${RDKAFKA_INCLUDE_DIR}" CACHE PATH "librdkafka include directory" FORCE)
            set(RDKAFKA_LIBRARY "${RDKAFKA_LIBRARY}" CACHE FILEPATH "Path to librdkafka library" FORCE)
            set(RDKAFKA_LIBRARY_DIR "${RDKAFKA_LIBRARY_DIR}" CACHE PATH "Directory containing librdkafka library" FORCE)
            message(STATUS "Using librdkafka from Julia artifact at ${RDKAFKA_LIBRARY_DIR}")
        else()
            message(WARNING "Unexpected librdkafka_jll output: '${_rdkafka_jll_raw}'")
        endif()
    else()
        message(WARNING "Failed to query librdkafka_jll artifact: ${_rdkafka_jll_error}")
    endif()
endif()

if(STATIC_DEPENDENCY_LINKING AND RDKAFKA_LIBRARY AND NOT RDKAFKA_LIBRARY MATCHES "\\.a$")
    find_library(RDKAFKA_STATIC_CANDIDATE
        NAMES rdkafka
        HINTS ${RDKAFKA_LIBRARY_DIR}
        PATH_SUFFIXES "" static lib lib64
    )
    if(RDKAFKA_STATIC_CANDIDATE)
        set(RDKAFKA_LIBRARY "${RDKAFKA_STATIC_CANDIDATE}")
        get_filename_component(RDKAFKA_LIBRARY_DIR "${RDKAFKA_LIBRARY}" DIRECTORY)
        message(STATUS "Static librdkafka detected at ${RDKAFKA_LIBRARY}")
    endif()
endif()

if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
    message(FATAL_ERROR "Could not locate librdkafka headers or library. Set RDKAFKA_ROOT (or RDKAFKA_INCLUDE_DIR / RDKAFKA_LIBRARY) to help CMake find them.")
endif()

if(STATIC_DEPENDENCY_LINKING AND NOT RDKAFKA_LIBRARY MATCHES "\\.a$")
    message(FATAL_ERROR "Static librdkafka required but found shared library: ${RDKAFKA_LIBRARY}. Provide a static build or set STATIC_DEPENDENCY_LINKING=OFF to allow shared libraries.")
endif()

set(RDKAFKA_HEADER_DIR "${RDKAFKA_INCLUDE_DIR}")
if(EXISTS "${RDKAFKA_HEADER_DIR}/rdkafka.h")
    # Header found directly in include directory (standard installation)
    set(RDKAFKA_INCLUDE_DIR "${RDKAFKA_HEADER_DIR}")
elseif(EXISTS "${RDKAFKA_HEADER_DIR}/librdkafka/rdkafka.h")
    # Header found in librdkafka subdirectory (as in Julia artifacts)
    # Keep RDKAFKA_INCLUDE_DIR as is, since includes use <librdkafka/rdkafka.h>
    set(RDKAFKA_INCLUDE_DIR "${RDKAFKA_HEADER_DIR}")
else()
    message(FATAL_ERROR "librdkafka header rdkafka.h not found in ${RDKAFKA_HEADER_DIR} or ${RDKAFKA_HEADER_DIR}/librdkafka")
endif()
message(STATUS "Using librdkafka headers from ${RDKAFKA_INCLUDE_DIR}")

get_filename_component(RDKAFKA_LIBRARY_DIR "${RDKAFKA_LIBRARY}" DIRECTORY)

set(SASL_ROOT "" CACHE PATH "Root directory of the libsasl2 (Cyrus SASL) installation")

set(_sasl_include_hints)
foreach(_hint ${SASL_ROOT} $ENV{SASL_ROOT} ${SASL_INCLUDEDIR} ${PKG_SASL_INCLUDE_DIRS})
    if(_hint)
        list(APPEND _sasl_include_hints "${_hint}")
    endif()
endforeach()
list(REMOVE_DUPLICATES _sasl_include_hints)

set(_sasl_library_hints)
foreach(_hint ${SASL_ROOT} $ENV{SASL_ROOT} ${SASL_LIBDIR} ${PKG_SASL_LIBRARY_DIRS})
    if(_hint)
        list(APPEND _sasl_library_hints "${_hint}")
    endif()
endforeach()
list(REMOVE_DUPLICATES _sasl_library_hints)

find_path(SASL_INCLUDE_DIR
    NAMES sasl/sasl.h
    HINTS ${_sasl_include_hints}
    PATH_SUFFIXES include include/sasl include/CyrusSASL
    PATHS
        /usr/local/include
        /usr/include
        /opt/homebrew/include
        /opt/local/include
)

find_library(SASL_LIBRARY
    NAMES sasl2 libsasl2
    HINTS ${_sasl_library_hints}
    PATH_SUFFIXES lib lib64 lib/x86_64-linux-gnu lib/aarch64-linux-gnu lib/arm64
    PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib64
        /opt/homebrew/lib
        /opt/local/lib
)

if(NOT SASL_LIBRARY)
    message(STATUS "Attempting to locate libsasl2 via Julia's CyrusSASL_jll artifact")
    set(_sasl_jll_script "
import Pkg
try
    using CyrusSASL_jll
catch err
    Pkg.instantiate()
    using CyrusSASL_jll
end
lib_path = CyrusSASL_jll.libsasl2_path
lib_dir = dirname(lib_path)
artifact_root = dirname(lib_dir)
include_dir = normpath(joinpath(artifact_root, \"include\"))
print(include_dir * \"\\n\" * lib_dir * \"\\n\" * lib_path)
")
    execute_process(
        COMMAND ${JULIA_BIN} "--project=${JULIA_PROJECT}" -e "${_sasl_jll_script}"
        OUTPUT_VARIABLE _sasl_jll_raw
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_VARIABLE _sasl_jll_error
        RESULT_VARIABLE _sasl_jll_result
    )
    if(_sasl_jll_result EQUAL 0)
        string(REPLACE "\r\n" "\n" _sasl_jll "${_sasl_jll_raw}")
        string(REPLACE "\r" "\n" _sasl_jll "${_sasl_jll}")
        string(REPLACE "\n" ";" _sasl_jll_list "${_sasl_jll}")
        list(LENGTH _sasl_jll_list _sasl_jll_len)
        if(_sasl_jll_len GREATER_EQUAL 3)
            list(GET _sasl_jll_list 0 SASL_INCLUDE_DIR)
            list(GET _sasl_jll_list 1 SASL_LIBRARY_DIR)
            list(GET _sasl_jll_list 2 SASL_LIBRARY)
            set(SASL_INCLUDE_DIR "${SASL_INCLUDE_DIR}" CACHE PATH "libsasl2 include directory" FORCE)
            set(SASL_LIBRARY "${SASL_LIBRARY}" CACHE FILEPATH "Path to libsasl2" FORCE)
            set(SASL_LIBRARY_DIR "${SASL_LIBRARY_DIR}" CACHE PATH "Directory containing libsasl2" FORCE)
            message(STATUS "Using libsasl2 from Julia artifact at ${SASL_LIBRARY_DIR}")
        else()
            message(WARNING "Unexpected CyrusSASL_jll output: '${_sasl_jll_raw}'")
        endif()
    else()
        message(WARNING "Failed to query CyrusSASL_jll artifact: ${_sasl_jll_error}")
    endif()
endif()

if(STATIC_DEPENDENCY_LINKING AND SASL_LIBRARY AND NOT SASL_LIBRARY MATCHES "\\.a$")
    find_library(SASL_STATIC_CANDIDATE
        NAMES sasl2 libsasl2
        HINTS ${SASL_LIBRARY_DIR}
        PATH_SUFFIXES "" static lib lib64
    )
    if(SASL_STATIC_CANDIDATE)
        set(SASL_LIBRARY "${SASL_STATIC_CANDIDATE}")
        get_filename_component(SASL_LIBRARY_DIR "${SASL_LIBRARY}" DIRECTORY)
        message(STATUS "Static libsasl2 detected at ${SASL_LIBRARY}")
    endif()
endif()

if(SASL_LIBRARY AND NOT SASL_LIBRARY_DIR)
    get_filename_component(SASL_LIBRARY_DIR "${SASL_LIBRARY}" DIRECTORY)
endif()

if(NOT SASL_LIBRARY)
    message(FATAL_ERROR "Could not locate libsasl2. Install Cyrus SASL on your system or set SASL_ROOT / SASL_LIBRARY to help CMake find it.")
endif()

if(STATIC_DEPENDENCY_LINKING AND NOT SASL_LIBRARY MATCHES "\\.a$")
    message(FATAL_ERROR "Static libsasl2 required but found shared library: ${SASL_LIBRARY}. Provide a static build or set STATIC_DEPENDENCY_LINKING=OFF to allow shared libraries.")
endif()

if(STATIC_DEPENDENCY_LINKING)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_original_find_library_suffixes})
    set(CMAKE_LINK_SEARCH_START_STATIC ${_original_link_search_start_static})
endif()

set(JLCXX_LINK_TARGETS)
foreach(_jl_target IN ITEMS
        JlCxx::cxxwrap_julia
        JlCxx::cxxwrap_julia_std
        JlCxx::cxxwrap_julia_stl)
    if(TARGET ${_jl_target})
        list(APPEND JLCXX_LINK_TARGETS ${_jl_target})
    endif()
endforeach()
list(REMOVE_DUPLICATES JLCXX_LINK_TARGETS)

if(NOT JLCXX_LINK_TARGETS)
    message(FATAL_ERROR "No usable JlCxx targets were exported by the detected CxxWrap installation.")
endif()

add_library(kafka SHARED main.cpp)
set_target_properties(kafka PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    PREFIX "lib"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
)

target_compile_definitions(kafka PRIVATE USE_JLCXX)
target_compile_options(kafka PRIVATE
    $<$<CONFIG:Release>:-O3>
    $<$<CONFIG:RelWithDebInfo>:-O2>
)

target_include_directories(kafka
    PRIVATE
        "${DEPS_ROOT}/include"
        "${CXXWRAP_PREFIX}/include"
        ${Julia_INCLUDE_DIRS}
        ${RDKAFKA_INCLUDE_DIR}
)

target_link_libraries(kafka
    PRIVATE
        ${JLCXX_LINK_TARGETS}
        ${RDKAFKA_LIBRARY}
        ${SASL_LIBRARY}
        Julia::Julia
        Threads::Threads
)

set(_runtime_rpaths "${CXXWRAP_PREFIX}/lib")
if(RDKAFKA_LIBRARY_DIR)
    list(APPEND _runtime_rpaths "${RDKAFKA_LIBRARY_DIR}")
endif()
if(Julia_LIBRARY_DIR)
    list(APPEND _runtime_rpaths "${Julia_LIBRARY_DIR}")
endif()
if(SASL_LIBRARY_DIR)
    list(APPEND _runtime_rpaths "${SASL_LIBRARY_DIR}")
endif()
list(REMOVE_DUPLICATES _runtime_rpaths)
set_target_properties(kafka PROPERTIES BUILD_RPATH "${_runtime_rpaths}")

if(APPLE)
    set(CMAKE_MACOSX_RPATH ON)
    set(_origin "@loader_path")
else()
    set(_origin "$ORIGIN")
endif()

set(_install_rpaths "${_origin}")
list(APPEND _install_rpaths ${_runtime_rpaths})
list(REMOVE_DUPLICATES _install_rpaths)

set_target_properties(kafka PROPERTIES INSTALL_RPATH "${_install_rpaths}")

set(CMAKE_INSTALL_RPATH "${_install_rpaths}")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

install(TARGETS
        kafka
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})