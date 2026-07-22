#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(nats_context SHARED ${SOURCES})
  target_link_libraries(nats_context PUBLIC cnats::nats)
  target_link_libraries(nats_context PRIVATE orbit_utils ${ORBIT_EXTRA_LIB})
  set_target_properties(nats_context PROPERTIES
    VERSION ${NATS_CONTEXT_VERSION_MAJOR}.${NATS_CONTEXT_VERSION_MINOR}.${NATS_CONTEXT_VERSION_PATCH}
    SOVERSION ${NATS_CONTEXT_VERSION_MAJOR}.${NATS_CONTEXT_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(nats_context PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_context DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(nats_context_static STATIC ${SOURCES})
  target_link_libraries(nats_context_static PUBLIC cnats::nats_static)
  target_link_libraries(nats_context_static PRIVATE orbit_utils_static ${ORBIT_EXTRA_LIB})
  target_compile_definitions(nats_context_static PRIVATE NATS_CONTEXT_STATIC)
  set_target_properties(nats_context_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(nats_context_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_context_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()

# --------------------------------------
# Install public headers
# --------------------------------------
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/context.h
  DESTINATION ${ORBIT_INCLUDE_DIR})
