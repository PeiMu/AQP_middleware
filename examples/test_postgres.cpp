/*
 * Entrance for testing DuckDB Adapter
 * */

#include "postgres_adapter.h"
#include "util.h"
#include <iostream>

int main() {
  std::string queries_dir =
      "/home/pei/Project/benchmarks/imdb_job-postgres/queries/";
  // Get all .sql files
  std::vector<std::string> sql_files;
  try {
    sql_files = middleware::get_sql_files(queries_dir);
  } catch (const std::exception &e) {
    std::cerr << "Error reading directory: " << e.what() << std::endl;
    return 1;
  }

  if (sql_files.empty()) {
    std::cerr << "No .sql files found in: " << queries_dir << std::endl;
    return 1;
  }

  try {
    // Create adapter
    auto adapter = std::make_unique<middleware::PostgreSQLAdapter>(
        "host=localhost port=5432 dbname=imdb user=imdb");

    for (const auto &sql_file_path : sql_files) {
      std::cout << "run query: " << sql_file_path << std::endl;
      std::ifstream sql_file(sql_file_path);
      // Parse SQL
      if (!sql_file.is_open()) {
        std::cerr << "Failed to open SQL file" << std::endl;
        return 1;
      }
      std::stringstream buffer;
      buffer << sql_file.rdbuf();
      std::string sql = buffer.str();
      adapter->ParseSQL(sql);

      // Convert logical plan to IR
      auto simplest_ir = adapter->ConvertPlanToIR();
#ifdef DEBUG
      std::cout << "\n=== Simplest IR ===" << std::endl;
      simplest_ir->Print();
      std::cout << "===================\n" << std::endl;
#endif

      // Convert IR to SQL
      sql = adapter->GenerateSQL(*simplest_ir, 1);
#ifdef DEBUG
      std::cout << "\n=== Generated SQL ===" << std::endl;
      std::cout << sql << std::endl;
      std::cout << "===================\n" << std::endl;
#endif

      // Execute and verify
      auto result = adapter->ExecuteSQL(sql);
      std::cout << "\nResults: " << result.num_rows << " rows" << std::endl;
      for (const auto &row : result.rows) {
        for (const auto &val : row) {
          std::cout << val << " ";
        }
        std::cout << std::endl;
      }
    }

    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;

    return 1;
  }
}