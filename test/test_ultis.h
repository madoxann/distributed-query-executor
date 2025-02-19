#include "arrow/array.h"
#include "arrow/table.h"
#include "gtest/gtest.h"

template <typename T>
void check_column_values(const std::shared_ptr<arrow::Array>& array, const std::vector<T>& expected_values) {
  auto typed_array = std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(array);
  ASSERT_EQ(typed_array->length(), expected_values.size()) << "Column length mismatch";

  for (int64_t i = 0; i < typed_array->length(); ++i) {
    ASSERT_EQ(typed_array->Value(i), expected_values[i]) << "Value mismatch at row " << i;
  }
}

inline void check_string_column_values(
    const std::shared_ptr<arrow::Array>& array,
    const std::vector<std::string>& expected_values
) {
  auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
  ASSERT_EQ(string_array->length(), expected_values.size()) << "Column length mismatch";

  for (int64_t i = 0; i < string_array->length(); ++i) {
    ASSERT_EQ(string_array->GetString(i), expected_values[i]) << "Value mismatch at row " << i;
  }
}

template <typename T>
void verify_column(
    const std::shared_ptr<arrow::Table>& table,
    int column_index,
    const std::vector<T>& expected_values
) {
  auto column = table->column(column_index);
  ASSERT_EQ(column->num_chunks(), 1) << "Expected single chunk";
  check_column_values(column->chunk(0), expected_values);
}

inline void verify_string_column(
    const std::shared_ptr<arrow::Table>& table,
    int column_index,
    const std::vector<std::string>& expected_values
) {
  auto column = table->column(column_index);
  ASSERT_EQ(column->num_chunks(), 1) << "Expected single chunk";
  check_string_column_values(column->chunk(0), expected_values);
}