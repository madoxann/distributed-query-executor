#pragma once

#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "statement.h"

#include <sqlite3.h>

#include <memory>

namespace arrow_sql_bridge {
class statement_batch_reader : public arrow::RecordBatchReader {
public:
  static arrow::Result<std::shared_ptr<statement_batch_reader>>
  make(const std::shared_ptr<arrow_sql_bridge::statement>& statement);

  std::shared_ptr<arrow::Schema> schema() const override;

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
  bool is_executed = false;
  int rc = SQLITE_OK;

  std::shared_ptr<statement> stmt_ptr;
  std::shared_ptr<arrow::Schema> schema_ptr;

  statement_batch_reader(std::shared_ptr<statement> statement, std::shared_ptr<arrow::Schema> schema);
};
} // namespace arrow_sql_bridge
