cmake_minimum_required(VERSION 3.10)

set(TINY_REDIS_NAME "tiny_redis")

project("${TINY_REDIS_NAME}" LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(TINY_REDIS_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(TINY_REDIS_SRC_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src")

option(TINY_REDIS_ENABLE_WARNINGS "Enable compiler warnings" ON)
find_package(Threads REQUIRED)

if(TINY_REDIS_ENABLE_WARNINGS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        add_compile_options(-Wall -Wextra -Wpedantic -Wconversion)
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        add_compile_options(/W4)
    endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -DNDEBUG)
endif()

include_directories(${TINY_REDIS_INCLUDE_PATH})
add_executable(${TINY_REDIS_NAME}
    "${TINY_REDIS_SRC_PATH}/main.cpp"
    "${TINY_REDIS_SRC_PATH}/resp.cpp"
    "${TINY_REDIS_SRC_PATH}/server.cpp"
    "${TINY_REDIS_SRC_PATH}/config_loader.cpp"
    "${TINY_REDIS_SRC_PATH}/aof.cpp"
    "${TINY_REDIS_SRC_PATH}/rdb.cpp"
    "${TINY_REDIS_SRC_PATH}/replica_client.cpp"
    "${TINY_REDIS_SRC_PATH}/kv.cpp"
    "${TINY_REDIS_SRC_PATH}/skiplist.cpp"
)

target_compile_definitions(${TINY_REDIS_NAME} PRIVATE $<$<CONFIG:Debug>:TINY_REDIS_DEBUG=1>)

target_include_directories(${TINY_REDIS_NAME} PRIVATE
    ${TINY_REDIS_INCLUDE_PATH})

install(TARGETS ${TINY_REDIS_NAME} RUNTIME DESTINATION bin)

target_link_libraries(${TINY_REDIS_NAME} PRIVATE Threads::Threads)
