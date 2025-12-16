/*
 * DBAdapter as an interface
 * */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "cpp_interface.h"

namespace middleware {
struct QueryResult {
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> rows;
  int num_rows;
  int num_columns;

  QueryResult() : num_rows(0), num_columns(0) {}
};

class DBAdapter {
public:
  DBAdapter() = default;

  virtual ~DBAdapter() = default;

  // Parse SQL and return logical plan
  virtual void ParseSQL(const std::string &sql) = 0;

  // Convert logical plan to IR
  virtual std::unique_ptr<ir_sql_converter::SimplestStmt> ConvertPlanToIR() = 0;

  // Convert IR to SQL
  std::string GenerateSQL(ir_sql_converter::SimplestStmt &simplest_stmt,
                     int query_id, bool save_file = false,
                     const std::string &sql_path = "") {
    auto sql = ir_sql_converter::ConvertIRToSQL(simplest_stmt, query_id,
                                                save_file, sql_path);
    return sql;
  }

  // Execute SQL query
  virtual QueryResult ExecuteSQL(const std::string &sql) = 0;
  virtual void ExecuteSQLandCreateTempTable(const std::string &sql) = 0;

  // Temp table management
  virtual void CreateTempTable(const std::string &table_name,
                               const QueryResult &result) = 0;

  virtual void DropTempTable(const std::string &table_name) = 0;

  virtual bool TempTableExists(const std::string &table_name) = 0;

  virtual std::string GetEngineName() const = 0;

  virtual void CleanUp() = 0;

  unsigned int subquery_index = 1;
};
} // namespace middleware