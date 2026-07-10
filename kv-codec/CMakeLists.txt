set(KV_CODEC_VERSION_MAJOR 0)
set(KV_CODEC_VERSION_MINOR 1)
set(KV_CODEC_VERSION_PATCH 0)

#----------------------------
# Add the project directories

add_subdirectory(src)

if(ORBIT_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
#----------------------------
