#---------------------------------------
# Grab all files in 'src'
#---------------------------------------
file(GLOB SOURCES "*.c")

# --------------------------------------
# Create the shared and static libraries
# --------------------------------------
if(ORBIT_BUILD_LIB_SHARED)
  add_library(kv_codec SHARED ${SOURCES})
  target_link_libraries(kv_codec PUBLIC cnats::nats)
  target_link_libraries(kv_codec PRIVATE orbit_utils ${ORBIT_EXTRA_LIB})
  set_target_properties(kv_codec PROPERTIES
    VERSION ${KV_CODEC_VERSION_MAJOR}.${KV_CODEC_VERSION_MINOR}.${KV_CODEC_VERSION_PATCH}
    SOVERSION ${KV_CODEC_VERSION_MAJOR}.${KV_CODEC_VERSION_MINOR}
    DEBUG_POSTFIX d)
  target_include_directories(kv_codec PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS kv_codec DESTINATION ${ORBIT_LIBDIR})
endif()

if(ORBIT_BUILD_LIB_STATIC)
  add_library(kv_codec_static STATIC ${SOURCES})
  target_link_libraries(kv_codec_static PUBLIC cnats::nats_static)
  target_link_libraries(kv_codec_static PRIVATE orbit_utils_static ${ORBIT_EXTRA_LIB})
  target_compile_definitions(kv_codec_static PRIVATE KV_CODEC_STATIC)
  set_target_properties(kv_codec_static PROPERTIES DEBUG_POSTFIX d)
  target_include_directories(kv_codec_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>)
  install(TARGETS kv_codec_static ARCHIVE DESTINATION ${ORBIT_LIBDIR})
endif()

# --------------------------------------
# Install public headers
# --------------------------------------
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/kvcodec.h
  DESTINATION ${ORBIT_INCLUDE_DIR})
