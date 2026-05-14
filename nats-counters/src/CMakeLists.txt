#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(nats_counters SHARED ${SOURCES})
  target_link_libraries(nats_counters PUBLIC cnats::nats)
  target_link_libraries(nats_counters PRIVATE jetstream_extra ${ORBIT_EXTRA_LIB})
  set_target_properties(nats_counters PROPERTIES
    VERSION ${NATS_COUNTERS_VERSION_MAJOR}.${NATS_COUNTERS_VERSION_MINOR}.${NATS_COUNTERS_VERSION_PATCH}
    SOVERSION ${NATS_COUNTERS_VERSION_MAJOR}.${NATS_COUNTERS_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(nats_counters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_counters DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(nats_counters_static STATIC ${SOURCES})
  target_link_libraries(nats_counters_static PUBLIC cnats::nats_static)
  target_link_libraries(nats_counters_static PRIVATE jetstream_extra_static ${ORBIT_EXTRA_LIB})
  target_compile_definitions(nats_counters_static PRIVATE NATS_COUNTERS_STATIC)
  set_target_properties(nats_counters_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(nats_counters_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_counters_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()
