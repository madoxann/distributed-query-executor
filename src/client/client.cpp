#include "client.h"

#include <iostream>

namespace flight = arrow::flight;

arrow::Result<std::shared_ptr<arrow::Table>>
execute_sql_query(const std::string& host, int port, const std::string& query, bool stdout_results) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp(host, port));
  flight::FlightClientOptions options;
  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location, options));

  flight::FlightCallOptions call_options;
  flight::sql::FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, query));

  const auto& endpoints = info->endpoints();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::shared_ptr<arrow::Schema> schema;

  for (const auto& endpoint : endpoints) {
    ARROW_ASSIGN_OR_RAISE(auto stream, sql_client.DoGet(call_options, endpoint.ticket));

    ARROW_ASSIGN_OR_RAISE(schema, stream->GetSchema());

    if (stdout_results) {
      std::cout << "Results from endpoint " << &endpoint - &endpoints[0] + 1 << " of " << endpoints.size() << std::endl;
      std::cout << "Schema:" << std::endl;
      std::cout << schema->ToString() << std::endl;
    }

    while (true) {
      ARROW_ASSIGN_OR_RAISE(flight::FlightStreamChunk chunk, stream->Next());
      if (!chunk.data) {
        break;
      }

      if (stdout_results) {
        std::cout << chunk.data->ToString();
      }

      batches.push_back(chunk.data);
    }

    if (stdout_results) {
      int64_t total_rows = 0;
      for (const auto& batch : batches) {
        total_rows += batch->num_rows();
      }
      std::cout << "Total rows: " << total_rows << std::endl;
    }
  }

  if (batches.empty()) {
    return arrow::Table::MakeEmpty(schema);
  }

  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batches));
  return table;
}
