#pragma once

#include "arrow/flight/sql/server.h"
#include "arrow/result.h"
#include "statement.h"
#include "statement_batch_reader.h"

#include <sqlite3.h>

namespace arrow_sql_bridge {
class flight_sql_server : public arrow::flight::sql::FlightSqlServerBase {
public:
  ~flight_sql_server() override = default;

  static arrow::Result<std::shared_ptr<flight_sql_server>> make(const std::string& path);

  arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQuery& command,
      const arrow::flight::FlightDescriptor& descriptor
  ) override;

  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQueryTicket& command
  ) override;

private:
  class impl;
  std::shared_ptr<impl> impl_ptr;

  explicit flight_sql_server(std::shared_ptr<impl> impl);
};
} // namespace arrow_sql_bridge
