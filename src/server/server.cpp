#include "server.h"

namespace fs = std::filesystem;
namespace flight = arrow::flight;

std::string get_env_or_default(const std::string& var_name, const std::string& default_value) {
  const char* value = std::getenv(var_name.c_str());
  return value ? std::string(value) : default_value;
}

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>>
build_server(const fs::path& database_filename, const std::string& hostname, int port) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp(hostname, port));

  std::cout << "Apache Arrow version: " << ARROW_VERSION_STRING << std::endl;

  flight::FlightServerOptions options(location);
  options.auth_handler = std::make_unique<flight::NoOpAuthHandler>();

  std::shared_ptr<arrow_sql_bridge::flight_sql_server> sqlite_server;
  ARROW_ASSIGN_OR_RAISE(sqlite_server, arrow_sql_bridge::flight_sql_server::make(database_filename));

  std::cout << "Using database file: " << database_filename << std::endl;

  ARROW_CHECK_OK(sqlite_server->Init(options));
  ARROW_CHECK_OK(sqlite_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Listening on " << sqlite_server->location().ToString() << std::endl;

  return sqlite_server;
}

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>>
create_server(fs::path& database_filename, std::string hostname, int port) {
  if (database_filename.empty()) {
    return arrow::Status::Invalid("The database filename was not provided!");
  }
  database_filename = fs::absolute(database_filename);

  if (hostname.empty()) {
    hostname = get_env_or_default(ENV_HOSTNAME_VAR, DEFAULT_HOSTNAME);
  }

  return build_server(database_filename, hostname, port);
}

int run_server(fs::path& database_filename, std::string hostname, int port) {
  auto server_result = create_server(database_filename, hostname, port);

  if (server_result.ok()) {
    auto server = server_result.ValueOrDie();
    std::cout << "Server started successfully" << std::endl;
    ARROW_CHECK_OK(server->Serve());
    return EXIT_SUCCESS;
  } else {
    std::cerr << "Error: " << server_result.status().ToString() << std::endl;
    return EXIT_FAILURE;
  }
}

int run_flight_sql_server(const std::string& db_filename, const std::string& hostname, int port) {
  fs::path database_filename = db_filename;
  return run_server(database_filename, hostname, port);
}
