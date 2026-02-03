/*
 * IR-level query splitter main pipeline
 */

#pragma once

#include "adapters/db_adapter.h"
#include "simplest_ir.h"
#include "split/fk_based_splitter.h"
#include "split/foreign_key_extractor.h"
#include "split/split_algorithm.h"
#include "split/topdown_splitter.h"
#include "util/param_config.h"
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

namespace middleware {

// Temp table information after executing a subquery
struct TempTableInfo {
  std::string table_name;
  unsigned int table_index;
  uint64_t cardinality;
  std::vector<std::string> column_names;

  TempTableInfo(std::string name, unsigned int idx, uint64_t card)
      : table_name(std::move(name)), table_index(idx), cardinality(card) {}
};

class IRQuerySplitter {
public:
  IRQuerySplitter(DBAdapter *adapter, const ParamConfig &config);
  ~IRQuerySplitter() = default;

  // Main entry: Execute query with optional splitting
  QueryResult ExecuteWithSplit(const std::string &sql);

  // Statistics
  int GetIterationCount() const { return iteration_count_; }
  std::vector<double> GetIterationTimes() const { return iteration_times_; }

private:
  // === Iterative Split-Execute Loop (DuckDB pattern) ===
  // Main loop: while (!IsComplete()) { ExtractSubquery → Execute → Merge }
  QueryResult
  ExecuteSplitLoop(std::unique_ptr<ir_sql_converter::SimplestStmt> whole_ir);

  // Single iteration: extract → execute → merge
  // Returns false when done
  bool ExecuteOneIteration(
      std::unique_ptr<ir_sql_converter::SimplestStmt> &remaining_ir);

  // Execute a sub-IR and create temp table
  // Similar to DuckDB: ExecuteRow() + MergeDataChunk()
  // Uses: adapter_->GenerateSQL() → adapter_->ExecuteSQLandCreateTempTable()
  TempTableInfo
  ExecuteSubIR(std::unique_ptr<ir_sql_converter::SimplestStmt> sub_ir,
               const std::set<unsigned int> &executed_table_indices);

  // Update remaining IR after executing a subquery
  // Replace executed tables with temp table reference
  void UpdateRemainingIR(
      std::unique_ptr<ir_sql_converter::SimplestStmt> &remaining_ir,
      const TempTableInfo &temp_table,
      ir_sql_converter::SimplestStmt *node_to_replace,
      const std::set<unsigned int> &old_table_indices);

  // Helper function to recursively replace a node with temp table
  bool
  ReplaceNodeWithTempTable(ir_sql_converter::SimplestStmt *current,
                           ir_sql_converter::SimplestStmt *node_to_replace,
                           const TempTableInfo &temp_table,
                           const std::set<unsigned int> &old_table_indices);

  // === Helper Functions ===
  std::string GenerateTempTableName();
  void PrintIterationInfo(int iteration, const std::string &info);

  // Check if remaining IR is trivial (just a temp table reference)
  // Returns the temp table name if trivial, empty string otherwise
  std::string GetTrivialTempTable(ir_sql_converter::SimplestStmt *ir) const;

  DBAdapter *adapter_;
  ParamConfig config_;
  std::unique_ptr<SplitAlgorithm> splitter_;

  // Iteration tracking
  int iteration_count_ = 0;
  std::vector<TempTableInfo> temp_tables_;
  std::vector<double> iteration_times_;
};

} // namespace middleware