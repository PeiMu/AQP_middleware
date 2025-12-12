/*
 * Entrance for testing DuckDB Adapter
 * */

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb_adapter.h"
#include <iostream>

int main() {
  try {
    // Create adapter
    auto adapter = std::make_unique<middleware::DuckDBAdapter>(
        "/home/pei/Project/duckdb/measure/imdb.db");

    // Parse SQL
    std::ifstream sql_file(
        "/home/pei/Project/benchmarks/imdb_job-postgres/queries/6d.sql");
    std::stringstream buffer;
    buffer << sql_file.rdbuf();
    std::string sql = buffer.str();
    void *plan_ptr = adapter->ParseSQL(sql);

    // Cast to LogicalOperator
    auto *logical_plan = static_cast<duckdb::LogicalOperator *>(plan_ptr);
    std::cout << "\n=== Init Logical Plan ===" << std::endl;
    logical_plan->Print();
    std::cout << "===================\n" << std::endl;

    // Pre optimizer
    logical_plan =
        static_cast<duckdb::LogicalOperator *>(adapter->PreOptimizePlan());
    std::cout << "\n=== Logical Plan After Pre Optimizer ===" << std::endl;
    logical_plan->Print();
    std::cout << "===================\n" << std::endl;

    // Convert logical plan to IR
    auto simplest_ir = adapter->ConvertPlanToIR();
    std::cout << "\n=== Simplest IR ===" << std::endl;
    simplest_ir->Print();
    std::cout << "===================\n" << std::endl;

    // Convert IR to SQL
    sql = adapter->GetSQL(*simplest_ir, 1);
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