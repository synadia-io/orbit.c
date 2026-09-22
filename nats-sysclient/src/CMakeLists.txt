#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(nats_sysclient SHARED ${SOURCES})
  target_link_libraries(nats_sysclient PUBLIC cnats::nats)
  target_link_libraries(nats_sysclient PRIVATE orbit_utils nats_extra ${ORBIT_EXTRA_LIB})
  set_target_properties(nats_sysclient PROPERTIES
    VERSION ${NATS_SYSCLIENT_VERSION_MAJOR}.${NATS_SYSCLIENT_VERSION_MINOR}.${NATS_SYSCLIENT_VERSION_PATCH}
    SOVERSION ${NATS_SYSCLIENT_VERSION_MAJOR}.${NATS_SYSCLIENT_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(nats_sysclient PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_sysclient DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(nats_sysclient_static STATIC ${SOURCES})
  target_link_libraries(nats_sysclient_static PUBLIC cnats::nats_static)
  target_link_libraries(nats_sysclient_static PRIVATE orbit_utils_static nats_extra_static ${ORBIT_EXTRA_LIB})
  target_compile_definitions(nats_sysclient_static PRIVATE NATS_SYSCLIENT_STATIC)
  set_target_properties(nats_sysclient_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(nats_sysclient_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS nats_sysclient_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()

# --------------------------------------
# Install public headers under nats-sysclient/ (generic names like varz.h)
# --------------------------------------
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/sysclient.h
  ${CMAKE_CURRENT_SOURCE_DIR}/healthz.h
  ${CMAKE_CURRENT_SOURCE_DIR}/statsz.h
  ${CMAKE_CURRENT_SOURCE_DIR}/varz.h
  ${CMAKE_CURRENT_SOURCE_DIR}/connz.h
  ${CMAKE_CURRENT_SOURCE_DIR}/subsz.h
  ${CMAKE_CURRENT_SOURCE_DIR}/jsz.h
  DESTINATION ${ORBIT_INCLUDE_DIR}/nats-sysclient)
