#pragma once

#include "simplest_ir.h"
#include <set>
#include <utility>
#include <vector>

namespace middleware {

class AQPSelector {
public:
  virtual ~AQPSelector() = default;

  // Batch-evaluate estimated costs for multiple clusters.
  // Default: empty — strategies that don't need cost estimation need not
  // override.
  virtual void
  BatchEvaluateSubIRCosts(const std::vector<std::vector<int>> &clusters,
                          ir_sql_converter::AQPStmt *ir,
                          std::vector<std::pair<double, double>> &results) {};

  // Find the IR node (LCA) that spans exactly the given cluster tables.
  virtual ir_sql_converter::AQPStmt *
  SelectSubIR(ir_sql_converter::AQPStmt *ir,
            const std::set<unsigned int> &cluster_tables) = 0;

protected:
  // Helpers used by SelectSubIR (moved from FKBasedSplitter)
  std::set<unsigned int>
  CollectTableIndices(const ir_sql_converter::AQPStmt *node) const;

  bool NodeContainsExactlyTables(ir_sql_converter::AQPStmt *node,
                                 const std::set<unsigned int> &target_tables);

  bool NodeContainsAnyTable(ir_sql_converter::AQPStmt *node,
                            const std::set<unsigned int> &target_tables);
};

} // namespace middleware
