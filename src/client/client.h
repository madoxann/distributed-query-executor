#pragma once

#include "arrow/flight/sql/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"

#include <memory>
#include <string>

arrow::Result<std::shared_ptr<arrow::Table>>
execute_sql_query(const std::string& host, int port, const std::string& query, bool stdout_results = false);
