/*
 * TopDown split strategy implementation
 * Splits query at pipeline breakers (joins, aggregates) in top-down order
 */

#pragma once

#include "split/ir_reorder_get.h"
#include "split/split_algorithm.h"
#include <queue>
#include <set>

namespace middleware {

class TopDownSplitter : public SplitAlgorithm {
public:
  explicit TopDownSplitter(DBAdapter *adapter, bool enable_reorder = true)
      : SplitAlgorithm(adapter), reorder_get_(adapter),
        enable_reorder_(enable_reorder) {}

  // Preprocess: Run IR-level ReorderGet if enabled
  void Preprocess(std::unique_ptr<ir_sql_converter::SimplestStmt> &ir) override;

  std::unique_ptr<SubqueryExtraction>
  ExtractNextSubquery(ir_sql_converter::SimplestStmt *remaining_ir) override;

  bool IsComplete(const ir_sql_converter::SimplestStmt *remaining_ir) override;

  std::string GetStrategyName() const override { return "TopDown"; }

private:
  // Traverse IR top-down to find next split point
  void VisitOperator(ir_sql_converter::SimplestStmt *node);

  // Identify pipeline breakers (joins, aggregates)
  bool IsPipelineBreaker(const ir_sql_converter::SimplestStmt *node) const;

  // Collect all table indices used in a subtree
  std::set<unsigned int>
  CollectTableIndices(const ir_sql_converter::SimplestStmt *node) const;

  // Count number of base tables (scans) in the IR
  int CountBaseTables(const ir_sql_converter::SimplestStmt *node) const;

  // Find the top-most pipeline breaker
  ir_sql_converter::SimplestStmt *
  FindTopPipelineBreaker(ir_sql_converter::SimplestStmt *node);

  // Extract projection head (columns needed by parent operations)
  std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>>
  ExtractProjectionHead(const ir_sql_converter::SimplestStmt *node);

  IRReorderGet reorder_get_;
  bool enable_reorder_;

  // Track which tables have been executed
  std::set<unsigned int> executed_tables_;

  // Iteration counter
  int split_iteration_ = 0;
};

} // namespace middleware
