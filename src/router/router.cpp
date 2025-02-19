#include "router.h"

namespace flight = arrow::flight;

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>>
create_router(std::string hostname, int port, const std::list<arrow::flight::Location>& nodes, uint8_t receiver) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp(hostname, port));

  flight::FlightServerOptions options(location);
  options.auth_handler = std::make_unique<flight::NoOpAuthHandler>();

  std::shared_ptr<arrow_sql_router::flight_sql_router> sqlite_router;
  ARROW_ASSIGN_OR_RAISE(sqlite_router, arrow_sql_router::flight_sql_router::make(nodes, receiver));

  ARROW_CHECK_OK(sqlite_router->Init(options));
  ARROW_CHECK_OK(sqlite_router->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Routing on " << sqlite_router->location().ToString() << std::endl;
  return sqlite_router;
}

int run_flight_sql_router(
    const std::string& hostname,
    int port,
    const std::list<arrow::flight::Location>& nodes,
    uint8_t receiver
) {
  auto router = create_router(hostname, port, nodes, receiver);

  if (router.ok()) {
    auto server = router.ValueOrDie();
    std::cout << "Routing started successfully" << std::endl;
    ARROW_CHECK_OK(server->Serve());
    return EXIT_SUCCESS;
  } else {
    std::cerr << "Error: " << router.status().ToString() << std::endl;
    return EXIT_FAILURE;
  }
}
