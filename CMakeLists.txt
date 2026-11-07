cmake_minimum_required(VERSION 3.16)
project(pubsub LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(pubsub main.cpp)

if(MSVC)
    target_compile_options(pubsub PRIVATE /W4 /permissive-)
else()
    target_compile_options(pubsub PRIVATE -Wall -Wextra -Wpedantic)
    find_package(Threads REQUIRED)
    target_link_libraries(pubsub PRIVATE Threads::Threads)
endif()
