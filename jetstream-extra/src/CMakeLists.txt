#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(jetstream_extra SHARED ${SOURCES})
  target_link_libraries(jetstream_extra PUBLIC cnats::nats)
  target_link_libraries(jetstream_extra PRIVATE orbit_utils ${ORBIT_EXTRA_LIB})
  set_target_properties(jetstream_extra PROPERTIES
    VERSION ${JETSTREAM_EXTRA_VERSION_MAJOR}.${JETSTREAM_EXTRA_VERSION_MINOR}.${JETSTREAM_EXTRA_VERSION_PATCH}
    SOVERSION ${JETSTREAM_EXTRA_VERSION_MAJOR}.${JETSTREAM_EXTRA_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(jetstream_extra PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS jetstream_extra DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(jetstream_extra_static STATIC ${SOURCES})
  target_link_libraries(jetstream_extra_static PUBLIC cnats::nats_static)
  target_link_libraries(jetstream_extra_static PRIVATE orbit_utils_static ${ORBIT_EXTRA_LIB})
  target_compile_definitions(jetstream_extra_static PRIVATE JETSTREAM_EXTRA_STATIC)
  set_target_properties(jetstream_extra_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(jetstream_extra_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS jetstream_extra_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()

# --------------------------------------
# Install public headers
# --------------------------------------
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/batch_fetch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/fast_publish.h
  DESTINATION ${ORBIT_INCLUDE_DIR})
