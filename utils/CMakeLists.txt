set(ORBIT_UTILS_VERSION_MAJOR 0)
set(ORBIT_UTILS_VERSION_MINOR 1)
set(ORBIT_UTILS_VERSION_PATCH 0)

# orbit_add_library(<library> <version-prefix> [DEPS <lib>...]
#                   [HEADERS <header>...] [HEADER_SUBDIR <dir>])
#
# Builds <library> (shared) and <library>_static from every .c file in the
# calling directory, versioned from <version-prefix>_VERSION_{MAJOR,MINOR,PATCH}.
# Both link orbit_utils and each of DEPS privately, picking the static variant
# for the static library. HEADERS are installed, under HEADER_SUBDIR when given.
function(orbit_add_library lib prefix)
  cmake_parse_arguments(ARG "" "HEADER_SUBDIR" "DEPS;HEADERS" ${ARGN})

  file(GLOB sources "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
  set(version ${${prefix}_VERSION_MAJOR}.${${prefix}_VERSION_MINOR}.${${prefix}_VERSION_PATCH})
  set(soversion ${${prefix}_VERSION_MAJOR}.${${prefix}_VERSION_MINOR})

  if(ORBIT_BUILD_LIB_SHARED)
    add_library(${lib} SHARED ${sources})
    target_link_libraries(${lib} PUBLIC cnats::nats)
    target_link_libraries(${lib} PRIVATE orbit_utils ${ARG_DEPS} ${ORBIT_EXTRA_LIB})
    set_target_properties(${lib} PROPERTIES
      VERSION ${version}
      SOVERSION ${soversion}
      DEBUG_POSTFIX d)
    target_include_directories(${lib} PUBLIC
          $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
          $<INSTALL_INTERFACE:include>)
    install(TARGETS ${lib} DESTINATION ${ORBIT_LIBDIR})
  endif()

  if(ORBIT_BUILD_LIB_STATIC)
    set(static_deps "")
    foreach(dep ${ARG_DEPS})
      list(APPEND static_deps ${dep}_static)
    endforeach()

    add_library(${lib}_static STATIC ${sources})
    target_link_libraries(${lib}_static PUBLIC cnats::nats_static)
    target_link_libraries(${lib}_static PRIVATE orbit_utils_static ${static_deps} ${ORBIT_EXTRA_LIB})
    target_compile_definitions(${lib}_static PRIVATE ${prefix}_STATIC)
    set_target_properties(${lib}_static PROPERTIES DEBUG_POSTFIX d)
    target_include_directories(${lib}_static PUBLIC
          $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
          $<INSTALL_INTERFACE:include>)
    install(TARGETS ${lib}_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
  endif()

  if(ARG_HEADERS)
    set(dest ${ORBIT_INCLUDE_DIR})
    if(ARG_HEADER_SUBDIR)
      set(dest ${dest}/${ARG_HEADER_SUBDIR})
    endif()
    list(TRANSFORM ARG_HEADERS PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/)
    install(FILES ${ARG_HEADERS} DESTINATION ${dest})
  endif()
endfunction()

# orbit_add_examples(<name> <library>)
#
# Builds one executable, <name>-<file>, per .c file in the calling directory,
# linked against <library> (or <library>_static with
# ORBIT_BUILD_STATIC_EXAMPLES).
function(orbit_add_examples name lib)
  if(NOT ORBIT_BUILD_STATIC_EXAMPLES AND NOT ORBIT_BUILD_LIB_SHARED)
    message(FATAL_ERROR
      "Building examples require shared library, or run CMake with -DORBIT_BUILD_STATIC_EXAMPLES=ON")
  endif()

  if(ORBIT_BUILD_STATIC_EXAMPLES)
    set(lib ${lib}_static)
  endif()

  file(GLOB sources RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/*.c")

  foreach(src ${sources})
    string(REPLACE ".c" "" example ${src})
    set(exe "${name}-${example}")

    add_executable(${exe} ${CMAKE_CURRENT_SOURCE_DIR}/${src})
    target_include_directories(${exe} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${exe} ${lib} ${ORBIT_EXTRA_LIB})
  endforeach()
endfunction()

#----------------------------
# Add the project directories

add_subdirectory(src)

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
#----------------------------
