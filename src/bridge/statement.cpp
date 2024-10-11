#include "statement.h"

std::shared_ptr<arrow::DataType> column_to_arrow_datatype(const int column_type) {
  switch (column_type) {
  case SQLITE_INTEGER:
    return arrow::int64();
  case SQLITE_FLOAT:
    return arrow::float64();
  case SQLITE_BLOB:
    return arrow::binary();
  case SQLITE_TEXT:
    return arrow::utf8();
  case SQLITE_NULL:
  default:
    return arrow::null();
  }
}

// clang-format off
arrow::Result<std::shared_ptr<arrow::DataType>> sqlite_to_arrow_datatype(const char* sqlite_type) {
  if (sqlite_type == nullptr || std::strlen(sqlite_type) == 0) {
    return arrow::null();
  }

  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
    return arrow::int64();
  } else if (boost::iequals(sqlite_type, "REAL")) {
    return arrow::float64();
  } else if (boost::iequals(sqlite_type, "BLOB")) {
    return arrow::binary();
  } else if (boost::iequals(sqlite_type, "TEXT")
             || boost::iequals(sqlite_type, "DATE")
             || boost::istarts_with(sqlite_type, "char")
             || boost::istarts_with(sqlite_type, "varchar")) {
    return arrow::utf8();
  }
  return arrow::Status::Invalid("Invalid SQLite type: ", sqlite_type);
}

inline std::shared_ptr<arrow::DataType> get_unknown_dense_union() {
  return arrow::dense_union({
      field("string", arrow::utf8()),
      field("bytes", arrow::binary()),
      field("bigint", arrow::int64()),
      field("double", arrow::float64()),
  });
}

// clang-format on

int get_precision(const int column_type) {
  switch (column_type) {
  case SQLITE_INTEGER:
    return 10;
  case SQLITE_FLOAT:
    return 15;
  default:
    return 0;
  }
}

arrow::flight::sql::ColumnMetadata build_column_meta(int column_type, const char* table) {
  arrow::flight::sql::ColumnMetadata::ColumnMetadataBuilder builder = arrow::flight::sql::ColumnMetadata::Builder();

  builder.Scale(15).IsAutoIncrement(false).IsReadOnly(false);
  if (table == NULLPTR) {
    return builder.Build();
  } else if (column_type == SQLITE_TEXT || column_type == SQLITE_BLOB) {
    std::string table_name(table);
    builder.TableName(table_name);
  } else {
    std::string table_name(table);
    builder.TableName(table_name).Precision(get_precision(column_type));
  }
  return builder.Build();
}

namespace arrow_sql_bridge {
arrow::Result<std::shared_ptr<statement>> statement::make(sqlite3* db, const std::string& sql) {
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, sql.c_str(), static_cast<int>(sql.size()), &stmt, NULLPTR);

  std::string err_msg;
  if (rc != SQLITE_OK) {
    err_msg += "Can't prepare statement: " + std::string(sqlite3_errmsg(db));
    goto cleanup;
  }

  try {
    return std::shared_ptr<statement>(new statement(db, stmt));
  } catch (...) {
    err_msg += "Failed to create statement, allocation failed";
    goto cleanup;
  }

cleanup:
  if (stmt != nullptr) {
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
      err_msg += "; Failed to cleanup SQLite statement: ";
      err_msg += std::string(sqlite3_errmsg(db));
    }
  }
  return arrow::Status::Invalid(err_msg);
}

arrow::Result<std::shared_ptr<arrow::Schema>> statement::get_schema() const {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  int column_count = sqlite3_column_count(stmt);
  for (int i = 0; i < column_count; i++) {
    const char* column_name = sqlite3_column_name(stmt, i);
    const int column_type = sqlite3_column_type(stmt, i);
    const char* table = sqlite3_column_table_name(stmt, i);
    std::shared_ptr<arrow::DataType> data_type = column_to_arrow_datatype(column_type);

    if (data_type->id() == arrow::Type::NA) {
      const char* column_decltype = sqlite3_column_decltype(stmt, i);
      if (column_decltype != NULLPTR) {
        ARROW_ASSIGN_OR_RAISE(data_type, sqlite_to_arrow_datatype(column_decltype));
      } else {
        data_type = get_unknown_dense_union();
      }
    }

    arrow::flight::sql::ColumnMetadata column_meta = build_column_meta(column_type, table);
    fields.push_back(arrow::field(column_name, data_type, column_meta.metadata_map()));
  }

  return arrow::schema(fields);
}

arrow::Result<int> statement::step() {
  int rc = sqlite3_step(stmt);
  if (rc == SQLITE_ERROR) {
    return arrow::Status::ExecutionError("A SQLite runtime error has occurred: ", sqlite3_errmsg(db));
  }

  return rc;
}

arrow::Result<int> statement::reset() {
  int rc = sqlite3_reset(stmt);
  if (rc == SQLITE_ERROR) {
    return arrow::Status::ExecutionError("A SQLite runtime error has occurred: ", sqlite3_errmsg(db));
  }

  return rc;
}

sqlite3_stmt* statement::get_sqlite3_statement() const {
  return stmt;
}

statement::~statement() noexcept {
  sqlite3_finalize(stmt);
}
} // namespace arrow_sql_bridge
