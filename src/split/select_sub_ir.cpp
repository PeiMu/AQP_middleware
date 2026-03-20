#include "split/select_sub_ir.h"

#include "simplest_ir.h"

namespace middleware {

std::set<unsigned int> AQPSelector::CollectTableIndices(
    const ir_sql_converter::AQPStmt *node) const {
  std::set<unsigned int> indices;

  if (!node)
    return indices;

  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    auto *scan = dynamic_cast<const ir_sql_converter::SimplestScan *>(node);
    if (scan) {
      indices.insert(scan->GetTableIndex());
    }
  }

  // ChunkNode is a constant value list used by Mark joins (IN clauses).
  // It is not a real relational table and must not be counted as one.

  for (const auto &child : node->children) {
    auto child_indices = CollectTableIndices(child.get());
    indices.insert(child_indices.begin(), child_indices.end());
  }

  return indices;
}

bool AQPSelector::NodeContainsAnyTable(
    ir_sql_converter::AQPStmt *node,
    const std::set<unsigned int> &target_tables) {
  if (!node)
    return false;

  // Check if this is a Scan node with a target table
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    auto *scan = dynamic_cast<ir_sql_converter::SimplestScan *>(node);
    if (scan && target_tables.count(scan->GetTableIndex())) {
      return true;
    }
  }

  // Check if this is a Chunk node (temp table)
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ChunkNode) {
    auto *chunk = dynamic_cast<ir_sql_converter::SimplestChunk *>(node);
    if (chunk && target_tables.count(chunk->GetTableIndex())) {
      return true;
    }
  }

  // Recursively check children
  for (const auto &child : node->children) {
    if (NodeContainsAnyTable(child.get(), target_tables)) {
      return true;
    }
  }

  return false;
}

bool AQPSelector::NodeContainsExactlyTables(
    ir_sql_converter::AQPStmt *node,
    const std::set<unsigned int> &target_tables) {
  if (!node)
    return false;

  // Collect all tables in this node
  auto node_tables = CollectTableIndices(node);

  // Check if the sets are equal
  return node_tables == target_tables;
}

} // namespace middleware
