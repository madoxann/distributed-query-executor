#include "flight_sql_router.h"

#include "arrow/flight/client.h"
#include "arrow/flight/sql/client.h"

namespace flight = arrow::flight;

namespace arrow_sql_router {
class flight_sql_router::impl {
private:
  std::vector<flight::Location> nodes;
  uint8_t receiver;
  std::unordered_map<std::string, std::unique_ptr<flight::sql::FlightSqlClient>> client_cache;

  static arrow::Result<flight::Ticket> make_ticket(const flight::Location& location, const std::string& query) {
    ARROW_ASSIGN_OR_RAISE(auto query_ticket, flight::sql::CreateStatementQueryTicket(query));
    return flight::Ticket{location.ToString() + "|" + std::move(query_ticket)};
  }

  arrow::Result<flight::sql::FlightSqlClient*> get_or_create_client(const flight::Location& location) {
    std::string loc_str = location.ToString();
    auto it = client_cache.find(loc_str);
    if (it != client_cache.end()) {
      return it->second.get();
    }

    flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location, options));
    auto sql_client = std::make_unique<flight::sql::FlightSqlClient>(std::move(client));
    auto sql_client_ptr = sql_client.get();
    client_cache[loc_str] = std::move(sql_client);
    return sql_client_ptr;
  }

public:
  explicit impl(std::vector<flight::Location> nodes, uint8_t receiver)
      : nodes(std::move(nodes))
      , receiver(receiver) {}

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext&,
      const flight::sql::StatementQuery& command,
      const flight::FlightDescriptor& descriptor
  ) {
    if (nodes.empty() || receiver >= nodes.size()) {
      return arrow::Status::Invalid("Invalid receiver index");
    }

    const auto& location = nodes[receiver];
    ARROW_ASSIGN_OR_RAISE(auto client, get_or_create_client(location));

    flight::FlightCallOptions call_options;
    ARROW_ASSIGN_OR_RAISE(auto info, client->Execute(call_options, command.query));
    ARROW_ASSIGN_OR_RAISE(auto ticket, make_ticket(location, command.query));
    // std::vector<flight::FlightEndpoint> endpoints{flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
    std::shared_ptr<arrow::Schema> schema = arrow::schema({});
    auto result = flight::FlightInfo::Make(
                      *schema,
                      descriptor,
                      info->endpoints(),
                      info->total_records(),
                      info->total_bytes(),
                      info->ordered()
    )
                      .ValueOrDie();
    return std::make_unique<arrow::flight::FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>>
  DoGetStatement(const flight::ServerCallContext&, const flight::sql::StatementQueryTicket& command) {
    std::string ticket_payload = command.statement_handle;
    size_t delimiter = ticket_payload.find('|');
    if (delimiter == std::string::npos) {
      return arrow::Status::Invalid("Invalid ticket format");
    }

    std::string location_str = ticket_payload.substr(0, delimiter);
    std::string query = ticket_payload.substr(delimiter + 1);
    ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::Parse(location_str));
    ARROW_ASSIGN_OR_RAISE(auto client, get_or_create_client(location));

    flight::FlightCallOptions call_options;
    flight::Ticket ticket{query};
    ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(call_options, ticket));

    std::shared_ptr<flight::MetadataRecordBatchReader> shared_reader = std::move(stream);
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, flight::MakeRecordBatchReader(shared_reader));
    return std::make_unique<flight::RecordBatchStream>(batch_reader);
  }
};

arrow::Result<std::shared_ptr<flight_sql_router>>
flight_sql_router::make(const std::list<flight::Location>& nodes, uint8_t receiver) {
  if (nodes.empty()) {
    return arrow::Status::Invalid("No nodes provided");
  }

  std::vector<flight::Location> nodes_vector(nodes.begin(), nodes.end());
  auto impl_ptr = std::make_shared<impl>(std::move(nodes_vector), receiver);
  return std::shared_ptr<flight_sql_router>(new flight_sql_router(std::move(impl_ptr)));
}

arrow::Result<std::unique_ptr<flight::FlightInfo>> flight_sql_router::GetFlightInfoStatement(
    const flight::ServerCallContext& context,
    const flight::sql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor
) {
  return impl_ptr->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>> flight_sql_router::DoGetStatement(
    const flight::ServerCallContext& context,
    const flight::sql::StatementQueryTicket& command
) {
  return impl_ptr->DoGetStatement(context, command);
}

flight_sql_router::flight_sql_router(std::shared_ptr<impl> impl)
    : impl_ptr(std::move(impl)) {}

} // namespace arrow_sql_router
