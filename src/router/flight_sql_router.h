#pragma once

#include "arrow/flight/sql/server.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "sqlite3.h"

#include <list>
#include <utility>

namespace arrow_sql_router {
class flight_sql_router : public arrow::flight::sql::FlightSqlServerBase {
public:
  ~flight_sql_router() override = default;

  static arrow::Result<std::shared_ptr<flight_sql_router>> make(
      const std::list<arrow::flight::Location>& nodes,
      uint8_t receiver
  ); // tmp receiver - No. of node that will answer

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

  explicit flight_sql_router(std::shared_ptr<impl> impl);
};
} // namespace arrow_sql_router