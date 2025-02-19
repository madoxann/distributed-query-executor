#include "../src/client/client.h"
#include "../src/router/router.h"
#include "../src/server/server.h"
#include "test_ultis.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

namespace fs = std::filesystem;
namespace flight = arrow::flight;

std::atomic<bool> running_n1(false), running_n2(false), running_router(false);

class RouterTest : public ::testing::Test {
protected:
  std::thread n1_thread, n2_thread, router_thread;
  std::shared_ptr<flight::sql::FlightSqlServerBase> n1, n2, router;
  const std::string hostname = "localhost";
  const int port_n1 = 31337, port_n2 = 31338, port_router = 31339;
  const fs::path db_path1 = "test.db_path1", db_path2 = "test.db_path2";

  void setup_node(
      fs::path db_path,
      int port,
      std::shared_ptr<flight::sql::FlightSqlServerBase>& server_ptr,
      std::thread& server_thread,
      std::atomic<bool>& running
  ) {
    auto server = create_server(db_path, hostname, port);
    if (!server.ok()) {
      std::cerr << "Failed to create test server: " << server.status().ToString() << std::endl;
      return;
    }

    server_ptr = std::move(server.ValueOrDie());
    server_thread = std::thread([&] {
      running.store(true);
      auto rc = server_ptr->Serve();
      if (!rc.ok()) {
        std::cerr << "Failed to start test server: " << rc.ToString() << std::endl;
      }
    });

    while (!running.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void setup_router(uint8_t receiver) {
    std::list<arrow::flight::Location> nodes{
        flight::Location::ForGrpcTcp(hostname, port_n1).ValueOrDie(),
        flight::Location::ForGrpcTcp(hostname, port_n2).ValueOrDie()
    };

    auto proxy = create_router(hostname, port_router, nodes, receiver);

    if (!proxy.ok()) {
      std::cerr << "Failed to create test proxy: " << proxy.status().ToString() << std::endl;
      return;
    }

    router = std::move(proxy.ValueOrDie());
    router_thread = std::thread([&] {
      running_router.store(true);
      auto rc = router->Serve();
      if (!rc.ok()) {
        std::cerr << "Failed to start test proxy: " << rc.ToString() << std::endl;
      }
    });

    while (!running_router.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void teardown_node(
      const fs::path& db_path,
      std::shared_ptr<flight::sql::FlightSqlServerBase>& server_ptr,
      std::thread& server_thread,
      std::atomic<bool>& running
  ) {
    auto _ = server_ptr->Shutdown();

    running.store(false);
    if (server_thread.joinable()) {
      server_thread.join();
    }

    if (!db_path.empty()) {
      std::remove(db_path.c_str());
    }
  }

  void SetUp() override {
    setup_node(db_path1, port_n1, n1, n1_thread, running_n1);
    setup_node(db_path2, port_n2, n2, n2_thread, running_n2);
  }

  void TearDown() override {
    teardown_node(db_path1, n1, n1_thread, running_n1);
    teardown_node(db_path2, n2, n2_thread, running_n2);
    teardown_node("", router, router_thread, running_router);
  }

  arrow::Result<std::shared_ptr<arrow::Table>> execute(const std::string& query, int port) {
    return execute_sql_query(hostname, port, query);
  }
};

TEST_F(RouterTest, SimpleRouting) {
  setup_router(1);
  auto status = execute("create table Groups (group_id int, group_no char(6));", port_n1);
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();
}

TEST_F(RouterTest, RoutingCorrectness) {
  setup_router(2);
  auto status = execute("create table Groups (group_id int, group_no char(6));", port_n2);
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("select * from Groups;", port_n1);
  ASSERT_FALSE(status.ok()) << "Table should not be created for db1";

  status = execute("select * from Groups;", port_n2);
  ASSERT_TRUE(status.ok()) << "Table was created for db2";
}

TEST_F(RouterTest, RoutingResults) {
  setup_router(1);
  auto status = execute("create table Groups (group_id int, group_no char(6));", port_n1);
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Groups values (1, 'M3132'), (2, 'M3435');", port_n1);
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute("select * from Groups;", port_n1);
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->num_rows(), 2);

  verify_column<int64_t>(table, 0, {1, 2});
  verify_string_column(table, 1, {"M3132", "M3435"});
}
