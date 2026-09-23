# orbit_add_testsuite(<target> <library> [PREFIX <prefix>])
#
# Builds <target> from test.c in the calling directory, links it against
# <library> (or <library>_static when only static libraries are built), and
# registers one ctest per _test(<Name>) entry in list_test.txt, named
# <prefix><Name>.
function(orbit_add_testsuite target lib)
  cmake_parse_arguments(ARG "" "PREFIX" "" ${ARGN})

  if(NOT ORBIT_BUILD_LIB_STATIC AND NOT ORBIT_BUILD_LIB_SHARED)
    message(FATAL_ERROR
      "Building tests requires a library target, or run CMake with -DBUILD_TESTING=OFF")
  endif()

  add_executable(${target} test.c)

  # Prefer shared library to avoid ASAN mismatch with static nats.c builds.
  if(ORBIT_BUILD_LIB_SHARED)
    target_link_libraries(${target} ${lib} ${ORBIT_EXTRA_LIB})
  else()
    target_link_libraries(${target} ${lib}_static ${ORBIT_EXTRA_LIB})
  endif()
  target_include_directories(${target} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${PROJECT_SOURCE_DIR}/utils/test)

  set(list ${CMAKE_CURRENT_SOURCE_DIR}/list_test.txt)
  target_compile_definitions(${target} PRIVATE ORBIT_TEST_LIST="${list}")
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${list})
  file(STRINGS ${list} entries)

  foreach(entry ${entries})
    string(REGEX REPLACE "_test\\(([^)]+)\\)" "\\1" name ${entry})
    set(test_name ${ARG_PREFIX}${name})

    add_test(NAME ${test_name}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMAND ${target} ${name})

    set_tests_properties(${test_name} PROPERTIES
      PASS_REGULAR_EXPRESSION "ALL PASSED"
      LABELS "test")

    if(ORBIT_SANITIZE)
      set_tests_properties(${test_name} PROPERTIES
        ENVIRONMENT "TSAN_OPTIONS=detect_deadlocks=1:second_deadlock_stack=1:halt_on_error=1:report_signal_unsafe=1")
    endif()
  endforeach()
endfunction()
