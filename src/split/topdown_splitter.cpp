/*
 * Implementation of TopDown split strategy
 */

#include "split/topdown_splitter.h"
#include <iostream>

namespace middleware {

void TopDownSplitter::Preprocess(
    std::unique_ptr<ir_sql_converter::SimplestStmt> &ir) {

  std::cout << "[TopDownSplitter] Preprocessing IR" << std::endl;

  if (enable_reorder_) {
    std::cout << "[TopDownSplitter] Running IR-level ReorderGet" << std::endl;
    auto reordered = reorder_get_.Reorder(std::move(ir));

    if (reordered) {
      ir = std::move(reordered);
      std::cout << "[TopDownSplitter] ReorderGet completed successfully"
                << std::endl;
    } else {
      std::cout
          << "[TopDownSplitter] ReorderGet returned null, using original IR"
          << std::endl;
    }
  } else {
    std::cout << "[TopDownSplitter] ReorderGet disabled, skipping" << std::endl;
  }

  // Reset state
  executed_tables_.clear();
  split_iteration_ = 0;
}

std::unique_ptr<SubqueryExtraction> TopDownSplitter::ExtractNextSubquery(
    ir_sql_converter::SimplestStmt *remaining_ir) {

  split_iteration_++;
  std::cout << "\n[TopDownSplitter] Iteration " << split_iteration_
            << ": Extracting next subquery" << std::endl;

  if (!remaining_ir) {
    std::cout << "[TopDownSplitter] Remaining IR is null" << std::endl;
    return nullptr;
  }

  // Find the top-most pipeline breaker
  auto *pipeline_breaker = FindTopPipelineBreaker(remaining_ir);

  if (!pipeline_breaker) {
    std::cout << "[TopDownSplitter] No pipeline breaker found, "
              << "extracting entire remaining IR" << std::endl;

    // No more splits - return the entire remaining IR
    auto table_indices = CollectTableIndices(remaining_ir);

    // TODO: Clone the remaining_ir since we can't move it
    // For now, return nullptr to indicate we need the whole query
    return nullptr;
  }

  std::cout << "[TopDownSplitter] Found pipeline breaker: "
            << (int)pipeline_breaker->GetNodeType() << std::endl;

  // Collect table indices involved in this split
  auto table_indices = CollectTableIndices(pipeline_breaker);

  std::cout << "[TopDownSplitter] Tables in this split: ";
  for (auto idx : table_indices) {
    std::cout << idx << " ";
  }
  std::cout << std::endl;

  // Extract projection head for this subquery
  auto projection_head = ExtractProjectionHead(pipeline_breaker);

  std::cout << "[TopDownSplitter] Projection head has "
            << projection_head.size() << " column(s)" << std::endl;

  // TODO: Create the sub-IR by cloning the pipeline breaker subtree
  // and wrapping it with a projection

  // For now, return nullptr as placeholder
  std::cout << "[TopDownSplitter] TODO: Implement sub-IR creation" << std::endl;

  std::string temp_table_name =
      "temp_topdown_" + std::to_string(split_iteration_);

  return std::make_unique<SubqueryExtraction>(nullptr, // TODO: actual sub-IR
                                              table_indices, temp_table_name);
}

bool TopDownSplitter::IsComplete(
    const ir_sql_converter::SimplestStmt *remaining_ir) {

  if (!remaining_ir) {
    return true;
  }

  // Check if only one base table remains
  int num_tables = CountBaseTables(remaining_ir);

  bool complete = (num_tables <= 1);

  std::cout << "[TopDownSplitter] IsComplete: " << num_tables
            << " table(s) remaining -> " << (complete ? "YES" : "NO")
            << std::endl;

  return complete;
}

bool TopDownSplitter::IsPipelineBreaker(
    const ir_sql_converter::SimplestStmt *node) const {

  if (!node)
    return false;

  auto node_type = node->GetNodeType();

  // Pipeline breakers: JOIN, AGGREGATE, (optionally FILTER)
  return node_type == ir_sql_converter::SimplestNodeType::JoinNode ||
         node_type == ir_sql_converter::SimplestNodeType::AggregateNode;

  // Note: DuckDB's implementation also considers FILTER with certain conditions
  // as pipeline breakers. We may need to add that logic here.
}

std::set<unsigned int> TopDownSplitter::CollectTableIndices(
    const ir_sql_converter::SimplestStmt *node) const {

  std::set<unsigned int> indices;

  if (!node)
    return indices;

  // If this is a scan node, add its table index
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    auto *scan = dynamic_cast<const ir_sql_converter::SimplestScan *>(node);
    if (scan) {
      indices.insert(scan->GetTableIndex());
    }
  }

  // Recursively collect from children
  for (const auto &child : node->children) {
    auto child_indices = CollectTableIndices(child.get());
    indices.insert(child_indices.begin(), child_indices.end());
  }

  return indices;
}

int TopDownSplitter::CountBaseTables(
    const ir_sql_converter::SimplestStmt *node) const {

  if (!node)
    return 0;

  int count = 0;

  // Count scan nodes
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    count = 1;
  }

  // Recursively count in children
  for (const auto &child : node->children) {
    count += CountBaseTables(child.get());
  }

  return count;
}

ir_sql_converter::SimplestStmt *
TopDownSplitter::FindTopPipelineBreaker(ir_sql_converter::SimplestStmt *node) {

  if (!node)
    return nullptr;

  // Check current node first (top-down)
  if (IsPipelineBreaker(node)) {
    return node;
  }

  // Recursively search children
  for (auto &child : node->children) {
    auto *breaker = FindTopPipelineBreaker(child.get());
    if (breaker) {
      return breaker;
    }
  }

  return nullptr;
}

std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>>
TopDownSplitter::ExtractProjectionHead(
    const ir_sql_converter::SimplestStmt *node) {

  std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>> projection;

  if (!node)
    return projection;

  // The projection head should include:
  // 1. All columns used in join conditions (if this is a join)
  // 2. All columns used in parent operations
  // 3. All columns in the current target list

  // For now, use the node's target list
  for (const auto &attr : node->target_list) {
    // Clone the attribute
    auto cloned = std::make_unique<ir_sql_converter::SimplestAttr>(*attr);
    projection.push_back(std::move(cloned));
  }

  return projection;
}

} // namespace middleware
