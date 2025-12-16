/*
 * Entrance for testing DuckDB Adapter
 * */

#include "postgres_adapter.h"
#include <iostream>

int main() {
  try {
    // Create adapter
    auto adapter = std::make_unique<middleware::PostgreSQLAdapter>(
        "host=localhost port=5432 dbname=imdb user=imdb");

    // Parse SQL
    std::ifstream sql_file(
        "/home/pei/Project/benchmarks/imdb_job-postgres/queries/6d.sql");
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
    std::cout << "\n=== Simplest IR ===" << std::endl;
    simplest_ir->Print();
    std::cout << "===================\n" << std::endl;

    // Convert IR to SQL
    sql = adapter->GenerateSQL(*simplest_ir, 1);
    std::cout << "\n=== Generated SQL ===" << std::endl;
    std::cout << sql << std::endl;
    std::cout << "===================\n" << std::endl;

    // Execute and verify
    auto result = adapter->ExecuteSQL(sql);
    std::cout << "\nResults: " << result.num_rows << " rows" << std::endl;
    for (const auto &row : result.rows) {
      for (const auto &val : row) {
        std::cout << val << " ";
      }
      std::cout << std::endl;
    }

    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}