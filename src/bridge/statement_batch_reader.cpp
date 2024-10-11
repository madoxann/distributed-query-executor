#include "statement_batch_reader.h"

#define INT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                                                     \
  case arrow::TYPE_CLASS##Type::type_id: {                                                                             \
    using c_type = typename arrow::TYPE_CLASS##Type::c_type;                                                           \
    auto status = int_builder_case<arrow::TYPE_CLASS##Builder, c_type>(array_builder, STMT, COLUMN);                   \
    ARROW_RETURN_NOT_OK(status);                                                                                       \
    break;                                                                                                             \
  }

#define FLOAT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                                                   \
  case arrow::TYPE_CLASS##Type::type_id: {                                                                             \
    auto status = float_builder_case<arrow::TYPE_CLASS##Builder>(array_builder, STMT, COLUMN);                         \
    ARROW_RETURN_NOT_OK(status);                                                                                       \
    break;                                                                                                             \
  }

#define STRING_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                                                  \
  case arrow::TYPE_CLASS##Type::type_id: {                                                                             \
    auto status = string_builder_case<arrow::TYPE_CLASS##Builder>(array_builder, STMT, COLUMN);                        \
    ARROW_RETURN_NOT_OK(status);                                                                                       \
    break;                                                                                                             \
  }

#define BINARY_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                                                  \
  case arrow::TYPE_CLASS##Type::type_id: {                                                                             \
    auto status = binary_builder_case<arrow::TYPE_CLASS##Builder>(array_builder, STMT, COLUMN);                        \
    ARROW_RETURN_NOT_OK(status);                                                                                       \
    break;                                                                                                             \
  }

template <typename BuilderType, typename ValueType>
arrow::Status int_builder_case(arrow::ArrayBuilder* array_builder, sqlite3_stmt* stmt, int col) {
  auto builder = reinterpret_cast<BuilderType*>(array_builder);
  const sqlite3_int64 value = sqlite3_column_int64(stmt, col);
  return builder->Append(static_cast<ValueType>(value));
}

template <typename BuilderType>
arrow::Status float_builder_case(arrow::ArrayBuilder* array_builder, sqlite3_stmt* stmt, int col) {
  auto builder = reinterpret_cast<BuilderType*>(array_builder);
  const double value = sqlite3_column_double(stmt, col);
  return builder->Append(static_cast<typename BuilderType::value_type>(value));
}

template <typename BuilderType>
arrow::Status string_builder_case(arrow::ArrayBuilder* array_builder, sqlite3_stmt* stmt, int col) {
  auto builder = reinterpret_cast<BuilderType*>(array_builder);
  const uint8_t* string = reinterpret_cast<const uint8_t*>(sqlite3_column_text(stmt, col));
  if (string == nullptr) {
    return builder->AppendNull();
  }
  const int bytes = sqlite3_column_bytes(stmt, col);
  return builder->Append(string, bytes);
}

template <typename BuilderType>
arrow::Status binary_builder_case(arrow::ArrayBuilder* array_builder, sqlite3_stmt* stmt, int col) {
  auto builder = reinterpret_cast<BuilderType*>(array_builder);
  const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, col));
  if (blob == nullptr) {
    return builder->AppendNull();
  }
  const int bytes = sqlite3_column_bytes(stmt, col);
  return builder->Append(blob, bytes);
}

namespace arrow_sql_bridge {
static constexpr int32_t kMaxBatchSize = 16384;

arrow::Result<std::shared_ptr<statement_batch_reader>>
statement_batch_reader::make(const std::shared_ptr<arrow_sql_bridge::statement>& statement) {
  ARROW_RETURN_NOT_OK(statement->reset());
  ARROW_ASSIGN_OR_RAISE(auto schema, statement->get_schema());

  try {
    return std::shared_ptr<statement_batch_reader>(new statement_batch_reader(statement, schema));
  } catch (...) {
    std::string err_msg("Failed to create batch_reader, allocation failed");
    return arrow::Status::OutOfMemory(err_msg);
  }
}

std::shared_ptr<arrow::Schema> statement_batch_reader::schema() const {
  return schema_ptr;
}

arrow::Status statement_batch_reader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
  sqlite3_stmt* sqlite3_stmt = stmt_ptr->get_sqlite3_statement();
  const int num_fields = schema_ptr->num_fields();

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);
  for (int i = 0; i < num_fields; i++) {
    const std::shared_ptr<arrow::Field>& field = schema_ptr->field(i);
    const std::shared_ptr<arrow::DataType>& field_type = field->type();

    ARROW_RETURN_NOT_OK(MakeBuilder(arrow::default_memory_pool(), field_type, &builders[i]));
  }

  int64_t rows = 0;
  if (!is_executed) {
    ARROW_ASSIGN_OR_RAISE(rc, stmt_ptr->reset());
    ARROW_ASSIGN_OR_RAISE(rc, stmt_ptr->step());
    is_executed = true;
  }

  while (rows < kMaxBatchSize && rc == SQLITE_ROW) {
    rows++;
    for (int i = 0; i < num_fields; i++) {
      const std::shared_ptr<arrow::Field>& field = schema_ptr->field(i);
      const std::shared_ptr<arrow::DataType>& field_type = field->type();
      auto array_builder = builders[i].get();

      if (sqlite3_column_type(sqlite3_stmt, i) == SQLITE_NULL) {
        ARROW_RETURN_NOT_OK(array_builder->AppendNull());
        continue;
      }

      switch (field_type->id()) {
        INT_BUILDER_CASE(Int64, sqlite3_stmt, i)
        INT_BUILDER_CASE(UInt64, sqlite3_stmt, i)
        INT_BUILDER_CASE(Int32, sqlite3_stmt, i)
        INT_BUILDER_CASE(UInt32, sqlite3_stmt, i)
        INT_BUILDER_CASE(Int16, sqlite3_stmt, i)
        INT_BUILDER_CASE(UInt16, sqlite3_stmt, i)
        INT_BUILDER_CASE(Int8, sqlite3_stmt, i)
        INT_BUILDER_CASE(UInt8, sqlite3_stmt, i)
        FLOAT_BUILDER_CASE(Double, sqlite3_stmt, i)
        FLOAT_BUILDER_CASE(Float, sqlite3_stmt, i)
        FLOAT_BUILDER_CASE(HalfFloat, sqlite3_stmt, i)
        BINARY_BUILDER_CASE(Binary, sqlite3_stmt, i)
        BINARY_BUILDER_CASE(LargeBinary, sqlite3_stmt, i)
        STRING_BUILDER_CASE(String, sqlite3_stmt, i)
        STRING_BUILDER_CASE(LargeString, sqlite3_stmt, i)
      default:
        return arrow::Status::NotImplemented("Not implemented SQLite data conversion to ", field_type->name());
      }
    }

    ARROW_ASSIGN_OR_RAISE(rc, stmt_ptr->step());
  }

  if (rows > 0) {
    std::vector<std::shared_ptr<arrow::Array>> columns(builders.size());
    for (int i = 0; i < num_fields; i++) {
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&columns[i]));
    }

    *out = arrow::RecordBatch::Make(schema_ptr, rows, columns);
  } else {
    *out = nullptr;
  }

  return arrow::Status::OK();
}

statement_batch_reader::statement_batch_reader(
    std::shared_ptr<statement> statement,
    std::shared_ptr<arrow::Schema> schema
)
    : stmt_ptr(std::move(statement))
    , schema_ptr(std::move(schema)) {}
} // namespace arrow_sql_bridge
