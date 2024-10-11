cmake_minimum_required(VERSION 3.15)
project(distributed-query-executor)

set(CMAKE_CXX_STANDARD 20)

file(GLOB BRIDGE_SRC src/bridge/*.cpp src/bridge/*.h)
file(GLOB CLIENT_SRC src/client/*.cpp src/client/*.h)
file(GLOB SERVER_SRC src/server/*.cpp src/server/*.h)
file(GLOB TEST_SRC test/*.cpp test/*.h)

add_executable(server ${BRIDGE_SRC} ${SERVER_SRC} src/server-main.cpp)
add_executable(client ${CLIENT_SRC} src/client-main.cpp)
add_executable(tests ${TEST_SRC} ${BRIDGE_SRC} ${CLIENT_SRC} ${SERVER_SRC})

include_directories(${CMAKE_SOURCE_DIR}/libs/include)

set(CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/libs" ${CMAKE_PREFIX_PATH})

find_package(SQLite3 REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)
find_package(GTest REQUIRED)
find_package(Boost REQUIRED COMPONENTS program_options)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(server PRIVATE Threads::Threads)
target_link_libraries(client PRIVATE Threads::Threads)
target_link_libraries(tests PRIVATE Threads::Threads)

target_link_libraries(server PUBLIC SQLite::SQLite3)
target_link_libraries(server PUBLIC Arrow::arrow_shared)
target_link_libraries(server PUBLIC ArrowFlight::arrow_flight_shared)
target_link_libraries(server PUBLIC ArrowFlightSql::arrow_flight_sql_shared)
target_link_libraries(server PUBLIC boost::boost)

target_link_libraries(client PUBLIC Arrow::arrow_shared)
target_link_libraries(client PUBLIC ArrowFlight::arrow_flight_shared)
target_link_libraries(client PUBLIC ArrowFlightSql::arrow_flight_sql_shared)
target_link_libraries(client PUBLIC boost::boost)

target_link_libraries(tests PUBLIC GTest::gtest GTest::gtest_main)
target_link_libraries(tests PUBLIC SQLite::SQLite3)
target_link_libraries(tests PUBLIC Arrow::arrow_shared)
target_link_libraries(tests PUBLIC ArrowFlight::arrow_flight_shared)
target_link_libraries(tests PUBLIC ArrowFlightSql::arrow_flight_sql_shared)
target_link_libraries(tests PUBLIC boost::boost)
