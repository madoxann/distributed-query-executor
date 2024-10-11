#include "../src/client/client.h"
#include "../src/server/server.h"

#include <arrow/table.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

std::atomic<bool> running(false);

class FlightSQLTest : public ::testing::Test {
protected:
  std::thread server_thread;
  std::shared_ptr<flight::sql::FlightSqlServerBase> server_ptr;
  std::string hostname = "localhost";
  int port = 31337;
  fs::path db_path = "test.db_path";

  void SetUp() override {
    auto server = create_server(db_path, hostname, port);
    if (!server.ok()) {
      std::cerr << "Failed to create test server: " << server.status().ToString() << std::endl;
      return;
    }

    server_ptr = std::move(server.ValueOrDie());
    server_thread = std::thread([this] {
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

  void TearDown() override {
    auto _ = server_ptr->Shutdown();

    running.store(false);
    if (server_thread.joinable()) {
      server_thread.join();
    }

    std::remove(db_path.c_str());
  }

  arrow::Result<std::shared_ptr<arrow::Table>> execute(const std::string& query) {
    return execute_sql_query(hostname, port, query);
  }
};

template <typename T>
void check_column_values(const std::shared_ptr<arrow::Array>& array, const std::vector<T>& expected_values) {
  auto typed_array = std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(array);
  ASSERT_EQ(typed_array->length(), expected_values.size()) << "Column length mismatch";

  for (int64_t i = 0; i < typed_array->length(); ++i) {
    ASSERT_EQ(typed_array->Value(i), expected_values[i]) << "Value mismatch at row " << i;
  }
}

void check_string_column_values(
    const std::shared_ptr<arrow::Array>& array,
    const std::vector<std::string>& expected_values
) {
  auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
  ASSERT_EQ(string_array->length(), expected_values.size()) << "Column length mismatch";

  for (int64_t i = 0; i < string_array->length(); ++i) {
    ASSERT_EQ(string_array->GetString(i), expected_values[i]) << "Value mismatch at row " << i;
  }
}

template <typename T>
void verify_column(
    const std::shared_ptr<arrow::Table>& table,
    int column_index,
    const std::vector<T>& expected_values
) {
  auto column = table->column(column_index);
  ASSERT_EQ(column->num_chunks(), 1) << "Expected single chunk";
  check_column_values(column->chunk(0), expected_values);
}

void verify_string_column(
    const std::shared_ptr<arrow::Table>& table,
    int column_index,
    const std::vector<std::string>& expected_values
) {
  auto column = table->column(column_index);
  ASSERT_EQ(column->num_chunks(), 1) << "Expected single chunk";
  check_string_column_values(column->chunk(0), expected_values);
}

TEST_F(FlightSQLTest, SimpleCorrectQueryTest) {
  auto status = execute("create table Groups (group_id int, group_no char(6));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("select * from Groups;");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();
}

TEST_F(FlightSQLTest, SimpleInvalidQueryTest) {
  auto status = execute("select * from Groups;");
  ASSERT_FALSE(status.ok()) << "Invalid query should fail, but it succeeded!";
}

TEST_F(FlightSQLTest, SimpleCorrectQueryValueTest) {
  auto status = execute("create table Groups (group_id int, group_no char(6));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Groups values (1, 'M3132'), (2, 'M3435');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(hostname, port, "select * from Groups;");
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->num_rows(), 2);

  verify_column<int64_t>(table, 0, {1, 2});
  verify_string_column(table, 1, {"M3132", "M3435"});
}

TEST_F(FlightSQLTest, UpdateQueryValueTest) {
  auto status = execute("create table Employees (id int, name char(20));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Employees values (1, 'Oleg Chalin'), (2, 'Alexey Il');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("update Employees set name = 'Oleg Ivanov' where id = 1;");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(hostname, port, "select * from Employees;");
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  verify_column<int64_t>(table, 0, {1, 2});
  verify_string_column(table, 1, {"Oleg Ivanov", "Alexey Il"});
}

TEST_F(FlightSQLTest, DeleteQueryValueTest) {
  auto status = execute("create table Users (id int, username char(20));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Users values (1, 'user1'), (2, 'user2'), (3, 'user3');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("delete from Users where id = 2;");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(hostname, port, "select * from Users;");
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  verify_column<int64_t>(table, 0, {1, 3});
  verify_string_column(table, 1, {"user1", "user3"});
}

TEST_F(FlightSQLTest, InsertSelectQueryValueTest) {
  auto status = execute("create table Source (id int, data char(10));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("create table Destination (id int, info char(10));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Source values (1, 'alpha'), (2, 'beta');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Destination (id, info) select id, data from Source;");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(hostname, port, "select * from Destination;");
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  verify_column<int64_t>(table, 0, {1, 2});
  verify_string_column(table, 1, {"alpha", "beta"});
}

TEST_F(FlightSQLTest, SelectWithConditionValueTest) {
  auto status = execute("create table Products (id int, price int);");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Products values (1, 99.99), (2, 149.49), (3, 199.99);");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(hostname, port, "select * from Products where price > 100 order by price desc;");
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  verify_column<int64_t>(table, 0, {3, 2});
  verify_column<int64_t>(table, 1, {199, 149});
}

TEST_F(FlightSQLTest, SelectWithJoinValueTest) {
  auto status = execute("create table Customers (id int, name char(20));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("create table Orders (customer_id int, product char(20));");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Customers values (1, 'Oleg'), (2, 'Alexey');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  status = execute("insert into Orders values (1, 'Multiplexer'), (2, 'Phone');");
  ASSERT_TRUE(status.ok()) << "Query execution failed: " << status.status().ToString();

  auto result = execute_sql_query(
      hostname,
      port,
      "select Customers.name, Orders.product from Customers join Orders on Customers.id = Orders.customer_id;"
  );
  ASSERT_TRUE(result.ok()) << "Query execution failed: " << result.status().ToString();

  auto table = result.ValueOrDie();
  verify_string_column(table, 0, {"Oleg", "Alexey"});
  verify_string_column(table, 1, {"Multiplexer", "Phone"});
}
