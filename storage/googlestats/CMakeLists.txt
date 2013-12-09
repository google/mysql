# Copyright 2013 Google Inc. All Rights Reserved.

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include
                    ${CMAKE_SOURCE_DIR}/sql
                    ${CMAKE_SOURCE_DIR}/regex)

ADD_DEFINITIONS(-DMYSQL_SERVER)

SET(GOOGLESTATS_SOURCES
    gss_aux.cc gss_cache.cc ha_googlestats.cc)

MYSQL_ADD_PLUGIN(googlestats ${GOOGLESTATS_SOURCES}
  STORAGE_ENGINE DEFAULT RECOMPILE_FOR_EMBEDDED)

TARGET_LINK_LIBRARIES(googlestats lzo2)
TARGET_LINK_LIBRARIES(googlestats_embedded lzo2)

