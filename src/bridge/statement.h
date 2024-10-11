#pragma once

#include "arrow/flight/sql/column_metadata.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

#include <boost/algorithm/string.hpp>
#include <sqlite3.h>

#include <memory>
#include <string>

namespace arrow_sql_bridge {
class statement {
public:
  static arrow::Result<std::shared_ptr<statement>> make(sqlite3* db, const std::string& sql);

  arrow::Result<std::shared_ptr<arrow::Schema>> get_schema() const;

  arrow::Result<int> step();

  arrow::Result<int> reset();

  sqlite3_stmt* get_sqlite3_statement() const;

  ~statement() noexcept;

private:
  sqlite3* db;
  sqlite3_stmt* stmt;

  statement(sqlite3* db, sqlite3_stmt* stmt)
      : db(db)
      , stmt(stmt) {}
};
} // namespace arrow_sql_bridge
