if(NOT BUILD_TESTING)
  return()
endif()

if(NOT ORBIT_BUILD_LIB_STATIC AND NOT ORBIT_BUILD_LIB_SHARED)
  message(FATAL_ERROR
    "Building tests requires a library target, or run CMake with -DBUILD_TESTING=OFF")
endif()

if(MSVC)
  set_source_files_properties(test.c PROPERTIES COMPILE_FLAGS "/w")
endif()

# Build the test program
add_executable(nats_context_testsuite test.c)

# Prefer shared library to avoid ASAN mismatch with static nats.c builds.
if(ORBIT_BUILD_LIB_SHARED)
  target_link_libraries(nats_context_testsuite nats_context ${ORBIT_EXTRA_LIB})
elseif(ORBIT_BUILD_LIB_STATIC)
  target_link_libraries(nats_context_testsuite nats_context_static ${ORBIT_EXTRA_LIB})
endif()
target_include_directories(nats_context_testsuite PRIVATE ${PROJECT_SOURCE_DIR}/nats-context/src)

set(TEST_LIST ${CMAKE_CURRENT_SOURCE_DIR}/list_test.txt)

set(TEST_NAMES)

# Get all the test names
if(EXISTS ${TEST_LIST})
  file(STRINGS ${TEST_LIST} TEST_NAMES)
endif()

foreach(name ${TEST_NAMES})
  # Remove the _test() prefix
  string(REGEX REPLACE "_test\\(([^)]+)\\)" "\\1" TEST_NAME ${name})
  add_test(NAME ${TEST_NAME}
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    COMMAND nats_context_testsuite ${TEST_NAME})

  # Make sure the test passes
  set_tests_properties(${TEST_NAME} PROPERTIES PASS_REGULAR_EXPRESSION "ALL PASSED")

  # Set TSAN_OPTIONS for the test
  if(ORBIT_SANITIZE)
    set_tests_properties(${TEST_NAME} PROPERTIES
      ENVIRONMENT "TSAN_OPTIONS=detect_deadlocks=1:second_deadlock_stack=1:halt_on_error=1:report_signal_unsafe=1")
  endif(ORBIT_SANITIZE)

  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "test")
endforeach()
