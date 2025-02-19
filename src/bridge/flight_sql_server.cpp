#include "flight_sql_server.h"

namespace flight = arrow::flight;

namespace arrow_sql_bridge {
class flight_sql_server::impl {
private:
  sqlite3* db;

  static arrow::Result<flight::Ticket> make_ticket(const std::string& query) {
    ARROW_ASSIGN_OR_RAISE(auto ticket_string, flight::sql::CreateStatementQueryTicket(query));
    return flight::Ticket{std::move(ticket_string)};
  }

public:
  explicit impl(sqlite3* db)
      : db(db) {}

  ~impl() {
    sqlite3_close(db);
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext&,
      const flight::sql::StatementQuery& command,
      const flight::FlightDescriptor& descriptor
  ) {
    const std::string& query = command.query;
    ARROW_ASSIGN_OR_RAISE(auto statement, arrow_sql_bridge::statement::make(db, query));
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->get_schema());
    ARROW_ASSIGN_OR_RAISE(auto ticket, make_ticket(query));
    std::vector<flight::FlightEndpoint> endpoints{flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
    const bool ordered = false;
    ARROW_ASSIGN_OR_RAISE(auto result, flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));

    return std::make_unique<flight::FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>>
  DoGetStatement(const flight::ServerCallContext&, const flight::sql::StatementQueryTicket& command) {
    const std::string& sql = command.statement_handle;

    std::shared_ptr<arrow_sql_bridge::statement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, arrow_sql_bridge::statement::make(db, sql));

    std::shared_ptr<arrow_sql_bridge::statement_batch_reader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, arrow_sql_bridge::statement_batch_reader::make(statement));

    return std::make_unique<flight::RecordBatchStream>(reader);
  }
};

arrow::Result<std::shared_ptr<flight_sql_server>> flight_sql_server::make(const std::string& path) {
  sqlite3* db = nullptr;
  char* db_location;
  bool in_memory = path.empty();

  if (in_memory) {
    db_location = (char*) ":memory:";
  } else {
    db_location = (char*) path.c_str();
  }

  if (sqlite3_open_v2(db_location, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, nullptr)) {
    std::string err_msg = "Can't open database: ";
    if (db != nullptr) {
      err_msg += sqlite3_errmsg(db);
      sqlite3_close(db);
    } else {
      err_msg += "Unable to start SQLite. Insufficient memory";
    }

    return arrow::Status::Invalid(err_msg);
  }

  auto impl_ptr = std::make_shared<impl>(db);

  try {
    return std::shared_ptr<flight_sql_server>(new flight_sql_server(std::move(impl_ptr)));
  } catch (...) {
    std::string err_msg("Failed to create flight_sql_server, allocation failed");
    return arrow::Status::OutOfMemory(err_msg);
  }
}

arrow::Result<std::unique_ptr<flight::FlightInfo>> flight_sql_server::GetFlightInfoStatement(
    const flight::ServerCallContext& context,
    const flight::sql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor
) {
  return impl_ptr->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>> flight_sql_server::DoGetStatement(
    const flight::ServerCallContext& context,
    const flight::sql::StatementQueryTicket& command
) {
  return impl_ptr->DoGetStatement(context, command);
}

flight_sql_server::flight_sql_server(std::shared_ptr<impl> impl)
    : impl_ptr(std::move(impl)) {}

} // namespace arrow_sql_bridge
