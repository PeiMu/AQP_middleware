/*
 * Base interface for query split algorithms
 */

#pragma once

#include "adapters/db_adapter.h"
#include "simplest_ir.h"
#include <memory>
#include <set>
#include <utility>

namespace middleware {

// Result of extracting a subquery
struct SubqueryExtraction {
  explicit SubqueryExtraction(std::set<unsigned int> table_indices,
                              std::string temp_name = "")
      : executed_table_indices(std::move(table_indices)),
        temp_table_name(std::move(temp_name)) {}

  // Get the IR to execute (prefers sub_ir over pipeline_breaker_ptr)
  ir_sql_converter::SimplestStmt *GetExecutableIR() const {
    if (sub_ir) {
      return sub_ir.get();
    }
    return pipeline_breaker_ptr;
  }

  // Set of table indices that will be executed in this subquery
  std::set<unsigned int> executed_table_indices;

  // Optional: name for the temporary table to create
  std::string temp_table_name;

  // The built sub-IR for this subquery (owns the IR)
  // For FK-based strategies: NEW sub-IR built from cluster tables (used for
  // execution) For TopDown: typically null (uses pipeline_breaker_ptr instead)
  std::unique_ptr<ir_sql_converter::SimplestStmt> sub_ir;

  // Pointer to node in the ORIGINAL tree that should be replaced with temp
  // table For FK-based strategies: the LCA node containing cluster tables (used
  // for UpdateRemainingIR) For TopDown: points to the subtree for both
  // execution AND replacement
  ir_sql_converter::SimplestStmt *pipeline_breaker_ptr = nullptr;
};

class SplitAlgorithm {
public:
  explicit SplitAlgorithm(DBAdapter *adapter) : adapter_(adapter) {}
  virtual ~SplitAlgorithm() = default;

  // Strategy-specific preprocessing (called once before splitting loop)
  // For TopDown: runs IR-level ReorderGet
  // For FK-based: extracts foreign keys
  virtual void Preprocess(std::unique_ptr<ir_sql_converter::SimplestStmt> &ir) {
  }

  // Extract next subquery to execute from the remaining IR
  // Returns: SubqueryExtraction with sub-IR and table indices
  // Returns nullptr when no more splits needed
  virtual std::unique_ptr<SubqueryExtraction>
  ExtractNextSubquery(ir_sql_converter::SimplestStmt *remaining_ir) = 0;

  // Check if splitting is complete (typically when only 1 table left)
  virtual bool
  IsComplete(const ir_sql_converter::SimplestStmt *remaining_ir) = 0;

  // Get strategy name for logging
  virtual std::string GetStrategyName() const = 0;

protected:
  DBAdapter *adapter_;

  // Iteration counter
  int split_iteration_ = 0;

  // Track which tables have been executed (now part of a temp table)
  std::set<unsigned int> executed_tables_;
};

} // namespace middleware
