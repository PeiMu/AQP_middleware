/*
 * Implementation of IR-level ReorderGet
 * Gets cardinality from IR itself, does NOT execute queries
 * File: /home/pei/Project/AQP_middleware/src/split/ir_reorder_get.cpp
 */

#include "split/ir_reorder_get.h"
#include <algorithm>
#include <iostream>
#include <map>
#include <stack>

namespace middleware {

std::unique_ptr<ir_sql_converter::SimplestStmt>
IRReorderGet::Reorder(std::unique_ptr<ir_sql_converter::SimplestStmt> ir) {
  std::cout << "[IRReorderGet] Starting table reordering by cardinality"
            << std::endl;

  // Step 1: Collect all table scans with their cardinalities
  std::vector<TableInfo> tables;
  CollectTableScans(ir.get(), tables);

  if (tables.size() <= 1) {
    std::cout << "[IRReorderGet] Only " << tables.size()
              << " table(s), no reordering needed" << std::endl;
    return ir;
  }

  std::cout << "[IRReorderGet] Found " << tables.size()
            << " tables:" << std::endl;
  for (const auto &table : tables) {
    std::cout << "  - Table " << table.table_index << " (" << table.table_name
              << "): cardinality = " << table.cardinality << std::endl;
  }

  // Step 2: Sort tables by cardinality (largest first for better join order)
  std::sort(tables.begin(), tables.end(),
            [](const TableInfo &a, const TableInfo &b) {
              return a.cardinality > b.cardinality; // Descending order
            });

  std::cout << "[IRReorderGet] Sorted order (largest first):" << std::endl;
  for (const auto &table : tables) {
    std::cout << "  - Table " << table.table_index << " (" << table.table_name
              << "): cardinality = " << table.cardinality << std::endl;
  }

  // Step 3: Collect join conditions
  std::vector<std::unique_ptr<ir_sql_converter::SimplestVarComparison>>
      join_conditions;
  CollectJoinConditions(ir.get(), join_conditions);

  std::cout << "[IRReorderGet] Found " << join_conditions.size()
            << " join condition(s)" << std::endl;

  // Step 4: Rebuild join tree with reordered tables
  // For now, we just record the reordering - actual tree rebuild requires
  // cloning
  std::cout << "[IRReorderGet] Reordering metadata updated" << std::endl;
  std::cout << "[IRReorderGet] Note: Full tree rebuilding not yet implemented"
            << std::endl;

  // Return original IR for now
  // TODO: Implement full tree rebuild with reordered tables
  return ir;
}

void IRReorderGet::CollectTableScans(ir_sql_converter::SimplestStmt *node,
                                     std::vector<TableInfo> &tables) {

  if (!node)
    return;

  // Check if this is a Scan node
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    auto *scan = dynamic_cast<ir_sql_converter::SimplestScan *>(node);
    if (scan) {
      // Get cardinality from the scan node itself (NO QUERY EXECUTION)
      uint64_t cardinality = scan->GetEstimatedCardinality();

      tables.emplace_back(
          scan->GetTableIndex(), cardinality, scan->GetTableName(),
          nullptr // Scan node will be reconstructed during rebuild
      );
    }
  }
  // Also check for Chunk nodes (intermediate results)
  else if (node->GetNodeType() ==
           ir_sql_converter::SimplestNodeType::ChunkNode) {
    auto *chunk = dynamic_cast<ir_sql_converter::SimplestChunk *>(node);
    if (chunk) {
      // For chunk nodes, we might need cardinality info as well
      // For now, treat them separately
      std::cout << "[IRReorderGet] Found Chunk node (table_index="
                << chunk->GetTableIndex() << ")" << std::endl;
    }
  }

  // Recursively collect from children
  for (auto &child : node->children) {
    CollectTableScans(child.get(), tables);
  }
}

void IRReorderGet::CollectJoinConditions(
    ir_sql_converter::SimplestStmt *node,
    std::vector<std::unique_ptr<ir_sql_converter::SimplestVarComparison>>
        &join_conds) {

  if (!node)
    return;

  // Check if this is a Join node
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::JoinNode) {
    auto *join = dynamic_cast<ir_sql_converter::SimplestJoin *>(node);
    if (join) {
      // Clone join conditions (for tree rebuild)
      for (const auto &cond : join->join_conditions) {
        auto cloned_cond =
            std::make_unique<ir_sql_converter::SimplestVarComparison>(
                cond->GetSimplestExprType(),
                std::make_unique<ir_sql_converter::SimplestAttr>(
                    *cond->left_attr),
                std::make_unique<ir_sql_converter::SimplestAttr>(
                    *cond->right_attr));
        join_conds.push_back(std::move(cloned_cond));
      }
    }
  }

  // Recursively collect from children
  for (auto &child : node->children) {
    CollectJoinConditions(child.get(), join_conds);
  }
}

std::unique_ptr<ir_sql_converter::SimplestStmt> IRReorderGet::RebuildJoinTree(
    std::vector<TableInfo> &sorted_tables,
    std::vector<std::unique_ptr<ir_sql_converter::SimplestVarComparison>>
        &join_conditions) {

  // Build a map of which tables participate in which join conditions
  // Map: (table_idx1, table_idx2) -> vector of join conditions
  std::map<std::pair<unsigned int, unsigned int>,
           std::vector<ir_sql_converter::SimplestVarComparison *>>
      join_map;

  for (const auto &cond : join_conditions) {
    unsigned int left_table = cond->left_attr->GetTableIndex();
    unsigned int right_table = cond->right_attr->GetTableIndex();

    // Add in both directions for lookup
    auto key1 = std::make_pair(left_table, right_table);
    auto key2 = std::make_pair(right_table, left_table);

    join_map[key1].push_back(cond.get());
    join_map[key2].push_back(cond.get());
  }

  // Implement join tree construction similar to DuckDB's ReorderGet::Optimize
  // Start with smallest table (last in sorted order after descending sort)
  // Build left-deep tree by connecting tables through join conditions

  std::cout << "[IRReorderGet] TODO: Implement join tree construction"
            << std::endl;
  std::cout << "[IRReorderGet] This requires:" << std::endl;
  std::cout << "  1. Creating new SimplestScan nodes for each table"
            << std::endl;
  std::cout << "  2. Building SimplestJoin nodes with appropriate conditions"
            << std::endl;
  std::cout << "  3. Handling CrossProduct for unconnected tables" << std::endl;

  return nullptr;
}

std::map<unsigned int, std::vector<ir_sql_converter::SimplestVarComparison *>>
IRReorderGet::BuildJoinMap(
    const std::vector<std::unique_ptr<ir_sql_converter::SimplestVarComparison>>
        &join_conds) {

  std::map<unsigned int, std::vector<ir_sql_converter::SimplestVarComparison *>>
      join_map;

  for (const auto &cond : join_conds) {
    // Add this join condition to both participating tables
    unsigned int left_table = cond->left_attr->GetTableIndex();
    unsigned int right_table = cond->right_attr->GetTableIndex();

    join_map[left_table].push_back(cond.get());
    join_map[right_table].push_back(cond.get());
  }

  return join_map;
}

} // namespace middleware
