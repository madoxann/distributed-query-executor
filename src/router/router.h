#include "arrow/flight/client.h"
#include "arrow/flight/sql/server.h"
#include "arrow/util/logging.h"
#include "flight_sql_router.h"

#include <cstdlib>
#include <iostream>

arrow::Result<std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>>
create_router(std::string hostname, int port, const std::list<arrow::flight::Location>& nodes, uint8_t receiver);

int run_flight_sql_router(
    const std::string& hostname,
    int port,
    const std::list<arrow::flight::Location>& nodes,
    uint8_t receiver
);
