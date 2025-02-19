cmake_minimum_required(VERSION 3.15)
project(distributed-query-executor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_VERBOSE_MAKEFILE TRUE)
find_package(SQLite3 REQUIRED)
find_package(Arrow REQUIRED)
find_package(GTest REQUIRED)
find_package(Boost 1.86.0 REQUIRED)
find_package(Boost REQUIRED COMPONENTS program_options)

file(GLOB BRIDGE_SRC src/bridge/*.cpp src/bridge/*.h)
file(GLOB CLIENT_SRC src/client/*.cpp src/client/*.h)
file(GLOB SERVER_SRC src/server/*.cpp src/server/*.h)
file(GLOB ROUTER_SRC src/router/*.cpp src/router/*.h)
file(GLOB TEST_SRC test/*.cpp test/*.h)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(server ${BRIDGE_SRC} ${SERVER_SRC} src/server-main.cpp)
target_link_libraries(server
        PRIVATE
        Threads::Threads
        SQLite::SQLite3
        arrow::arrow
        Boost::boost
)

add_executable(client ${CLIENT_SRC} src/client-main.cpp)
target_link_libraries(client
        PRIVATE
        Threads::Threads
        SQLite::SQLite3
        arrow::arrow
        Boost::boost
)

add_executable(tests ${TEST_SRC} ${BRIDGE_SRC} ${CLIENT_SRC} ${SERVER_SRC} ${ROUTER_SRC})
#add_executable(tests test/proxy-test.cpp ${BRIDGE_SRC} ${CLIENT_SRC} ${SERVER_SRC} ${ROUTER_SRC})
target_link_libraries(tests
        PRIVATE
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
        SQLite::SQLite3
        arrow::arrow
        Boost::boost
)
