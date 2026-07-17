if(NOT ORBIT_BUILD_EXAMPLES)
  return()
endif()
if(NOT ORBIT_BUILD_STATIC_EXAMPLES AND NOT ORBIT_BUILD_LIB_SHARED)
  message(FATAL_ERROR
    "Building examples require shared library, or run CMake with -DORBIT_BUILD_STATIC_EXAMPLES=ON")
endif()

# Get all the .c files in the examples directory
file(GLOB EXAMPLES_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.c)

# For each file...
foreach(examples_src ${EXAMPLES_SOURCES})

    # Remove the suffix so that it becomes the executable name
    string(REPLACE ".c" "" examplename ${examples_src})
    set(exampleexe "kv-codec-${examplename}")

    # Build the executable
    add_executable(${exampleexe} ${CMAKE_CURRENT_SOURCE_DIR}/${examples_src})

    target_include_directories(${exampleexe} PRIVATE
      ${PROJECT_SOURCE_DIR}/kv-codec/src
      ${CMAKE_CURRENT_SOURCE_DIR})

    # Link
    if(ORBIT_BUILD_STATIC_EXAMPLES)
      target_link_libraries(${exampleexe} kv_codec_static ${ORBIT_EXTRA_LIB})
    else()
      target_link_libraries(${exampleexe} kv_codec ${ORBIT_EXTRA_LIB})
    endif()

endforeach()
