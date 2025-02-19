#include "../bridge/flight_sql_server.h"
#include "arrow/flight/client.h"
#include "arrow/flight/sql/server.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"
#include "boost/program_options.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>

constexpr int DEFAULT_FLIGHT_PORT = 31337;
const std::string DEFAULT_HOSTNAME = "0.0.0.0";
const std::string ENV_HOSTNAME_VAR = "HOSTNAME";

arrow::Result<std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>>
create_server(std::filesystem::path& database_filename, std::string hostname, int port);

int run_flight_sql_server(const std::string& db_filename, const std::string& hostname, int port);