cmake_minimum_required(VERSION 3.16)
project(kvserver_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(kvserver_cpp
    src/main.cpp
    src/http_server.cpp
    src/db.cpp
)

target_include_directories(kvserver_cpp PRIVATE include src)

# pthread
find_package(Threads REQUIRED)
target_link_libraries(kvserver_cpp PRIVATE Threads::Threads)

# mysqlclient
find_library(MYSQLCLIENT_LIBRARY NAMES mysqlclient)
find_path(MYSQLCLIENT_INCLUDE_DIR mysql/mysql.h)
if(NOT MYSQLCLIENT_LIBRARY OR NOT MYSQLCLIENT_INCLUDE_DIR)
  message(FATAL_ERROR "mysqlclient not found. Install libmysqlclient-dev.")
endif()
target_include_directories(kvserver_cpp PRIVATE ${MYSQLCLIENT_INCLUDE_DIR})
target_link_libraries(kvserver_cpp PRIVATE ${MYSQLCLIENT_LIBRARY})

# extra warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(kvserver_cpp PRIVATE -Wall -Wextra -Wpedantic)
endif()
