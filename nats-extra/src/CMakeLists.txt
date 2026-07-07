#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(nats_extra SHARED ${SOURCES})
  target_link_libraries(nats_extra PUBLIC cnats::nats)
  target_link_libraries(nats_extra PRIVATE ${ORBIT_EXTRA_LIB})
  set_target_properties(nats_extra PROPERTIES
    VERSION ${NATS_EXTRA_VERSION_MAJOR}.${NATS_EXTRA_VERSION_MINOR}.${NATS_EXTRA_VERSION_PATCH}
    SOVERSION ${NATS_EXTRA_VERSION_MAJOR}.${NATS_EXTRA_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(nats_extra PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_extra DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(nats_extra_static STATIC ${SOURCES})
  target_link_libraries(nats_extra_static PUBLIC cnats::nats_static)
  target_link_libraries(nats_extra_static PRIVATE ${ORBIT_EXTRA_LIB})
  target_compile_definitions(nats_extra_static PRIVATE NATS_EXTRA_STATIC)
  set_target_properties(nats_extra_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(nats_extra_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_extra_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()

# --------------------------------------
# Install public headers
# --------------------------------------
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/requestmany.h
  DESTINATION ${ORBIT_INCLUDE_DIR})
