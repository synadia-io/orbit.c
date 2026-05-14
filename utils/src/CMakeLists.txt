#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
#
# These are internal helpers shared across orbit sub-libraries.
# The headers are intentionally NOT installed: only BUILD_INTERFACE
# include paths are exposed, and no install(FILES ...) is performed.
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(orbit_utils SHARED ${SOURCES})
  target_link_libraries(orbit_utils PUBLIC cnats::nats)
  target_link_libraries(orbit_utils PRIVATE ${ORBIT_EXTRA_LIB})
  set_target_properties(orbit_utils PROPERTIES
    VERSION ${ORBIT_UTILS_VERSION_MAJOR}.${ORBIT_UTILS_VERSION_MINOR}.${ORBIT_UTILS_VERSION_PATCH}
    SOVERSION ${ORBIT_UTILS_VERSION_MAJOR}.${ORBIT_UTILS_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(orbit_utils PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
  install(TARGETS orbit_utils DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(orbit_utils_static STATIC ${SOURCES})
  target_link_libraries(orbit_utils_static PUBLIC cnats::nats_static)
  target_link_libraries(orbit_utils_static PRIVATE ${ORBIT_EXTRA_LIB})
  target_compile_definitions(orbit_utils_static PRIVATE ORBIT_UTILS_STATIC)
  set_target_properties(orbit_utils_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(orbit_utils_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
  install(TARGETS orbit_utils_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()
