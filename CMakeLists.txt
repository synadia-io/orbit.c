cmake_minimum_required(VERSION 3.13)

project(orbit.c)
include(CTest)

# Uncomment to have the build process verbose
# set(CMAKE_VERBOSE_MAKEFILE TRUE)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Set output directories for libraries and executables.
# This is important for Windows builds to have the DLLs in the same directory as the executables.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Set a default build type if none was specified
set(default_build_type "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Release" "Debug" "MinSizeRel" "RelWithDebInfo")
endif()

option(ORBIT_COVERAGE "Code coverage" OFF)
option(ORBIT_BUILD_DEV_MODE "Enable type safety checks" OFF)
option(ORBIT_BUILD_EXAMPLES "Build examples" OFF)
option(ORBIT_BUILD_STATIC_EXAMPLES "Statically link examples" OFF)
option(ORBIT_BUILD_LIB_STATIC "Build static library" ON)
option(ORBIT_BUILD_LIB_SHARED "Build shared library" ON)
option(ORBIT_COMPILER_HARDENING "Compiler hardening flags" OFF)

if(DEFINED ENV{ORBIT_SANITIZE})
    set(ORBIT_SANITIZE "$ENV{ORBIT_SANITIZE}")
endif()

# cnats's cmake config references OpenSSL and Threads targets directly
# without using find_dependency, so we must find them first.
find_package(Threads REQUIRED)
find_package(OpenSSL)
find_package(cnats REQUIRED)

if(ORBIT_COVERAGE)
    message("-- Coverage Enabled")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
endif()

if(ORBIT_BUILD_DEV_MODE)
    add_definitions(-DDEV_MODE)
endif()

# Platform specific settings
if(UNIX)
    #---------------------------------------------------------------------------
    # Define ORBIT cache variables that override the CMAKE and MEMCHECK variables
    #---------------------------------------------------------------------------
    include(GNUInstallDirs)

    set(ORBIT_LIBDIR ${CMAKE_INSTALL_LIBDIR})
    set(ORBIT_INCLUDE_DIR ${CMAKE_INSTALL_INCLUDEDIR})

    set(ORBIT_BUILD_ARCH "64" CACHE STRING "32 for 32bits builds")

    find_program(MEMORYCHECK_COMMAND valgrind)
    set(ORBIT_MEMCHECK_CMD_OPTS "--leak-check=full --track-fds=yes --show-reachable=yes --num-callers=50" CACHE STRING "Memcheck options")
    set(MEMORYCHECK_COMMAND_OPTIONS ${ORBIT_MEMCHECK_CMD_OPTS} CACHE INTERNAL "")

    set(ORBIT_COMMON_C_FLAGS "-pedantic")

    # Some compiler hardening flags.
    if(ORBIT_COMPILER_HARDENING)
        set(ORBIT_COMMON_C_FLAGS "${ORBIT_COMMON_C_FLAGS} -fPIE -fstack-protector-all -D_FORTIFY_SOURCE=2 -O1")
        if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
            set(ORBIT_COMMON_C_FLAGS "${ORBIT_COMMON_C_FLAGS} -Wl,-z,relro,-z,now -pie")
        endif()
    endif()

    set(ORBIT_WARNINGS "-fstrict-aliasing -Wall -W -Wno-unused-parameter -Wno-unused-function -Wstrict-prototypes -Wwrite-strings")

    if(APPLE)
        set(ORBIT_OS "DARWIN")
        set(CMAKE_MACOSX_RPATH ON)
    else()
        set(ORBIT_OS "LINUX")
        if(NOT ANDROID)
            set(ORBIT_EXTRA_LIB "rt")
        endif()
        add_compile_definitions(_GNU_SOURCE)
    endif()

    if(${ORBIT_BUILD_ARCH} MATCHES "32")
        if(NOT APPLE)
            message("-----------------------------------------------------------")
            message("If build fails, you probably need to install libc6-dev-i386")
            message("apt-get install libc6-dev-i386")
            message("-----------------------------------------------------------")
        endif()

        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
        set(CMAKE_C_LINKER_FLAGS "${CMAKE_C_LINKER_FLAGS} -m32")
    endif()

elseif(WIN32)
    set(ORBIT_LIBDIR "lib")
    set(ORBIT_INCLUDE_DIR "include")
    set(ORBIT_EXTRA_LIB "ws2_32")
    set(ORBIT_OS "_WIN32")

    # Warning control.
    if(MSVC)
        # Set warning level to maximum, then disable:
        # /wd4100 unreferenced formal parameter
        # /wd4200 nonstandard extension used: zero-sized array in struct/union
        # /wd4130 logical operation on address of string constant
        # /wd4127 conditional expression is constant
        add_compile_options(/W4 /wd4100 /wd4200 /wd4130 /wd4127)
    else()
        add_compile_options(-Wall -Wextra -Wno-unused-parameter -Wno-pedantic -Wno-address -Wno-constant-conditional)
    endif()
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${ORBIT_COMMON_C_FLAGS} ${ORBIT_WARNINGS}")
if(ORBIT_SANITIZE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fno-optimize-sibling-calls")
endif()

add_compile_definitions(${ORBIT_OS} _REENTRANT)

#----------------------------
# Add the project directories

add_subdirectory(nats-counters)
#----------------------------
