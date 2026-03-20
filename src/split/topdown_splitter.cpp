/*
 * Implementation of TopDown split strategy
 */

#include "split/topdown_splitter.h"
#include <functional>
#include <iostream>

namespace middleware {

void TopDownSplitter::Preprocess(
    std::unique_ptr<ir_sql_converter::AQPStmt> &ir) {

  std::cout << "[TopDownSplitter] Preprocessing IR" << std::endl;

  // Reset state
  executed_tables_.clear();
  found_split_node_ = nullptr;
  query_split_index_ = 0;
  split_iteration_ = 0;
  max_table_index_ = 0;

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

  // Collect table names and find max index
  table_index_to_name_.clear();
  CollectTableNames(ir.get());
  for (const auto &[idx, name] : table_index_to_name_) {
    if (idx > max_table_index_) {
      max_table_index_ = idx;
    }
  }
  std::cout << "[TopDownSplitter] Max table index: " << max_table_index_
            << std::endl;

  std::cout << "[TopDownSplitter] Finish Preprocess with IR: "
            << ir->Print(true) << std::endl;
}

void TopDownSplitter::SplitIR(ir_sql_converter::AQPStmt *node) {
  if (!node || found_split_node_) {
    return;
  }

  // Process children from RIGHT to LEFT (following DuckDB pattern).
  // This ensures the build side (right child) is considered first in
  // left-deep plans, matching DuckDB's VisitOperator traversal order.
  for (int idx = node->children.size() - 1; idx >= 0; idx--) {
    auto *child = node->children[idx].get();
    if (!child) {
      continue;
    }

    auto child_type = child->GetNodeType();
    bool should_split = false;

    // ── STEP 1: Decide split (BEFORE recursing) ──────────────────────────
    // This mirrors DuckDB's VisitOperator which sets child->split_index
    // before calling VisitOperator(*child).  The key effect is that
    // top_most_ is updated here, so the recursive call already sees the
    // updated value (just like DuckDB's top_most member variable).
    switch (child_type) {

    case ir_sql_converter::SimplestNodeType::FilterNode: {
      // DuckDB lines 182-191 (follow_pipeline_breaker=true path):
      //   if (top_most && 0 == idx) → split, top_most=false
      // DuckDB lines 211-213:
      //   if (follow_pipeline_breaker_ && 0 == idx) → break (NO split)
      // So a FILTER only becomes a subquery when it is the very first
      // split-worthy node we encounter in the traversal (top_most_=true)
      // and it is a left/only child (idx==0).  Inner FILTERs that have
      // been pushed below a JOIN by FilterPushdown are NOT split.
      if (top_most_ && idx == 0) {
        top_most_ = false;
        should_split = true;
        std::cout << "[TopDownSplitter] Found FILTER split point at child "
                  << idx << " (top-most)" << std::endl;
      }
      // else: non-top-most FILTER at idx==0 → follow_pipeline_breaker break
      break;
    }

    case ir_sql_converter::SimplestNodeType::JoinNode: {
      // DuckDB lines 239-274.
      auto *join = dynamic_cast<ir_sql_converter::SimplestJoin *>(child);
      if (join) {
        auto join_type = join->GetSimplestJoinType();
        if (join_type == ir_sql_converter::SimplestJoinType::Semi ||
            join_type == ir_sql_converter::SimplestJoinType::Mark) {
          // DuckDB lines 243-246: skip SEMI/MARK, set split_index=0
          std::cout << "[TopDownSplitter] Skipping SEMI/MARK join" << std::endl;
        } else {
          // DuckDB lines 248-253: split if top_most || right child (idx==1)
          if (top_most_ || idx == 1) {
            should_split = true;
            std::cout << "[TopDownSplitter] Found JOIN split point at child "
                      << idx << (top_most_ ? " (top-most)" : " (build side)")
                      << std::endl;
          }
          top_most_ =
              false; // DuckDB line 274: always clear after any INNER JOIN
        }
      }
      break;
    }

    case ir_sql_converter::SimplestNodeType::CrossProductNode: {
      // DuckDB lines 277-285: CROSS_PRODUCT only acts as a sibling marker
      // (sets split_index = current, no increment) when
      // follow_pipeline_breaker. In the middleware's single-split-per-call
      // design this is a no-op.
      break;
    }

    default:
      break;
    }

    // ── STEP 2: Recurse into child (AFTER split decision) ────────────────
    // By recursing after the decision, top_most_ is already false when
    // children of a JOIN are visited, so pushed-down FILTERs inside the
    // join tree will not be mistakenly picked as split points.
    SplitIR(child);

    // ── STEP 3: Record the split point and stop ───────────────────────────
    if (should_split) {
      query_split_index_++;
      found_split_node_ = child;
      std::cout << "[TopDownSplitter] Added split point #" << query_split_index_
                << ": " << GetNodeTypeName(child_type) << std::endl;
      return; // One split per SplitIR call — stop here.
    }
  }
}

std::unique_ptr<SubqueryExtraction> TopDownSplitter::SplitIR(
    ir_sql_converter::AQPStmt *remaining_ir) {

  split_iteration_++;
  std::cout << "\n[TopDownSplitter] Iteration " << split_iteration_
            << ": Extracting next subquery" << std::endl;

  if (!remaining_ir) {
    std::cout << "[TopDownSplitter] Remaining IR is null" << std::endl;
    return nullptr;
  }

  // Re-visit the UPDATED tree to find the next split point.
  // Reset top_most_ to true before each traversal, mirroring DuckDB's
  // TopDownSplit::Clear() which resets the top_most member between iterations.
  found_split_node_ = nullptr;
  top_most_ = true;
  SplitIR(remaining_ir);

  // Check if there are more subqueries to execute
  if (!found_split_node_) {
    std::cout << "[TopDownSplitter] No more subqueries in queue" << std::endl;
    return nullptr;
  }

  std::cout << "[TopDownSplitter] Selected subquery node: "
            << GetNodeTypeName(found_split_node_->GetNodeType()) << std::endl;

  // Collect all table indices in this subquery's subtree
  auto table_indices = CollectTableIndices(found_split_node_);

  std::cout << "[TopDownSplitter] Tables involved: ";
  for (auto idx : table_indices) {
    std::cout << idx << " ";
    executed_tables_.insert(idx);
  }
  std::cout << "(" << table_indices.size() << " tables)" << std::endl;

  std::string temp_table_name = "temp_" + std::to_string(split_iteration_);

  // Create extraction info
  auto extraction =
      std::make_unique<SubqueryExtraction>(table_indices, temp_table_name);

  // Wrap found_split_node_ in a Projection with only the columns needed by
  // the remaining IR.  This gives the sub-query a well-defined SELECT list and
  // minimises the columns stored in the temp table.
  auto required_attrs = CollectRequiredAttrs(remaining_ir, table_indices);
  std::cout << "[TopDownSplitter] Wrapping split node in Projection with "
            << required_attrs.size() << " required column(s)" << std::endl;
  WrapInProjection(remaining_ir, std::move(required_attrs));
  // found_split_node_ now points to the new Projection node

  // Store pointer to the projection (used as both executable IR and replace
  // target in UpdateRemainingIR)
  extraction->pipeline_breaker_ptr = found_split_node_;

  std::cout << "[TopDownSplitter] Extraction complete for "
            << GetNodeTypeName(found_split_node_->GetNodeType()) << std::endl;

  // Check for same-table issue
  // DuckDB's same-table handling is also commented out in top_down.cpp — just
  // warn and continue rather than crashing.
  std::unordered_set<std::string> table_names_in_subquery;
  if (CheckSameTableInSubtree(found_split_node_, table_names_in_subquery)) {
    std::cerr << "[TopDownSplitter] Warning: same table appears multiple times "
                 "in subquery subtree; same-table merge not yet implemented"
              << std::endl;
  }

  return extraction;
}

bool TopDownSplitter::CheckSameTableInSubtree(
    ir_sql_converter::AQPStmt *node,
    std::unordered_set<std::string> &seen_tables) const {

  if (!node)
    return false;

  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ScanNode) {
    auto *scan = dynamic_cast<ir_sql_converter::SimplestScan *>(node);
    if (scan) {
      std::string table_name = scan->GetTableName();
      if (seen_tables.count(table_name)) {
        return true; // DUPLICATE FOUND
      }
      seen_tables.insert(table_name);
    }
  }

  for (const auto &child : node->children) {
    if (CheckSameTableInSubtree(child.get(), seen_tables)) {
      return true;
    }
  }

  return false;
}

bool TopDownSplitter::IsComplete(
    const ir_sql_converter::AQPStmt *remaining_ir) {

  if (!remaining_ir) {
    return true;
  }

  // Count remaining base tables - complete when only 1 table left
  int remaining_tables = CountBaseTables(remaining_ir);
  bool complete = (remaining_tables <= 1);

  std::cout << "[TopDownSplitter] IsComplete: " << (complete ? "YES" : "NO")
            << " (remaining tables: " << remaining_tables << ")" << std::endl;

  return complete;
}

std::set<unsigned int> TopDownSplitter::CollectTableIndices(
    const ir_sql_converter::AQPStmt *node) const {

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

  // If this is a chunk node (temp table), add its index
  if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::ChunkNode) {
    auto *chunk = dynamic_cast<const ir_sql_converter::SimplestChunk *>(node);
    if (chunk) {
      indices.insert(chunk->GetTableIndex());
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
    const ir_sql_converter::AQPStmt *node) const {

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

std::string TopDownSplitter::GetNodeTypeName(
    ir_sql_converter::SimplestNodeType type) const {
  switch (type) {
  case ir_sql_converter::SimplestNodeType::JoinNode:
    return "JOIN";
  case ir_sql_converter::SimplestNodeType::AggregateNode:
    return "AGGREGATE";
  case ir_sql_converter::SimplestNodeType::FilterNode:
    return "FILTER";
  case ir_sql_converter::SimplestNodeType::ScanNode:
    return "SCAN";
  case ir_sql_converter::SimplestNodeType::ProjectionNode:
    return "PROJECTION";
  case ir_sql_converter::SimplestNodeType::CrossProductNode:
    return "CROSS_PRODUCT";
  case ir_sql_converter::SimplestNodeType::OrderNode:
    return "ORDER";
  case ir_sql_converter::SimplestNodeType::LimitNode:
    return "LIMIT";
  case ir_sql_converter::SimplestNodeType::ChunkNode:
    return "CHUNK (temp table)";
  default:
    return "UNKNOWN(" + std::to_string((int)type) + ")";
  }
}

std::unique_ptr<ir_sql_converter::AQPStmt>
TopDownSplitter::UpdateRemainingIR(
    std::unique_ptr<ir_sql_converter::AQPStmt> remaining_ir,
    const std::set<unsigned int> &executed_table_indices,
    unsigned int temp_table_index, const std::string &temp_table_name,
    uint64_t temp_table_cardinality,
    const std::vector<std::pair<unsigned int, unsigned int>> &column_mappings,
    const std::vector<std::string> &column_names) {

  std::cout << "[TopDownSplitter::UpdateRemainingIR] Replacing executed "
               "subtree with temp table: "
            << temp_table_name << " (index " << temp_table_index
            << ", cardinality " << temp_table_cardinality << ")" << std::endl;

  if (!remaining_ir || !found_split_node_) {
    std::cerr << "[TopDownSplitter::UpdateRemainingIR] Error: null "
                 "remaining_ir or found_split_node_"
              << std::endl;
    return nullptr;
  }

  // Get raw pointer for tree traversal (we still own the IR)
  auto *ir_ptr = remaining_ir.get();

  // Helper lambda to find parent and replace child
  std::function<bool(ir_sql_converter::AQPStmt *)> ReplaceInTree;
  ReplaceInTree = [&](ir_sql_converter::AQPStmt *node) -> bool {
    if (!node)
      return false;

    for (size_t i = 0; i < node->children.size(); i++) {
      if (node->children[i].get() == found_split_node_) {
        // Found the split node - create SimplestScan to replace it
        std::cout
            << "[TopDownSplitter::UpdateRemainingIR] Found split node at child "
            << i << ", replacing with SimplestScan for temp table" << std::endl;

        // Build target list using pre-computed column names
        std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>>
            scan_target_list;
        for (size_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
          ir_sql_converter::SimplestVarType col_type =
              ir_sql_converter::SimplestVarType::IntVar;
          if (col_idx < found_split_node_->target_list.size()) {
            col_type = found_split_node_->target_list[col_idx]->GetType();
          }

          auto attr = std::make_unique<ir_sql_converter::SimplestAttr>(
              col_type, temp_table_index, static_cast<unsigned int>(col_idx),
              column_names[col_idx]);
          scan_target_list.push_back(std::move(attr));
        }

        // Create base AQPStmt for the scan
        std::vector<std::unique_ptr<ir_sql_converter::AQPStmt>>
            empty_children;
        auto scan_base = std::make_unique<ir_sql_converter::AQPStmt>(
            std::move(empty_children), std::move(scan_target_list),
            ir_sql_converter::SimplestNodeType::ScanNode);

        // Create SimplestScan node for temp table (treat it like a base table)
        auto scan_node = std::make_unique<ir_sql_converter::SimplestScan>(
            std::move(scan_base), temp_table_index, temp_table_name);
        scan_node->SetEstimatedCardinality(temp_table_cardinality);

        // Replace the child
        node->children[i] = std::move(scan_node);

        std::cout << "[TopDownSplitter::UpdateRemainingIR] Successfully "
                     "replaced subtree"
                  << std::endl;
        return true;
      }

      // Recursively search in children
      if (ReplaceInTree(node->children[i].get())) {
        return true;
      }
    }

    return false;
  };

  // Find and replace the split node in tree
  // Note: The case where remaining_ir == found_split_node_ should not happen
  // because IsComplete returns true when only 1 table remains (like DuckDB line
  // 605)
  if (!ReplaceInTree(ir_ptr)) {
    std::cerr << "[TopDownSplitter::UpdateRemainingIR] Warning: Could not find "
                 "split node in tree"
              << std::endl;
  }

  // Return the modified IR (same IR, modified in-place)
  return remaining_ir;
}

std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>>
TopDownSplitter::CollectRequiredAttrs(
    const ir_sql_converter::AQPStmt *full_ir,
    const std::set<unsigned int> &subquery_tables) const {

  std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>> required_attrs;
  std::set<std::pair<unsigned int, unsigned int>> seen_attrs;

  auto AddIfSubqueryAttr = [&](const ir_sql_converter::SimplestAttr *attr) {
    if (!attr)
      return;
    if (subquery_tables.count(attr->GetTableIndex())) {
      auto key = std::make_pair(attr->GetTableIndex(), attr->GetColumnIndex());
      if (!seen_attrs.count(key)) {
        seen_attrs.insert(key);
        required_attrs.push_back(
            std::make_unique<ir_sql_converter::SimplestAttr>(*attr));
      }
    }
  };

  // (a) Top-level target_list attrs that come from subquery tables
  for (const auto &attr : full_ir->target_list) {
    AddIfSubqueryAttr(attr.get());
  }

  // (b) AGGR/ORDER node attrs that reference subquery tables
  std::function<void(const ir_sql_converter::AQPStmt *)> CollectPlanAttrs;
  CollectPlanAttrs = [&](const ir_sql_converter::AQPStmt *node) {
    if (!node)
      return;
    if (node->GetNodeType() ==
        ir_sql_converter::SimplestNodeType::AggregateNode) {
      auto *agg =
          dynamic_cast<const ir_sql_converter::SimplestAggregate *>(node);
      if (agg) {
        for (const auto &fn_pair : agg->agg_fns) {
          AddIfSubqueryAttr(fn_pair.first.get());
        }
        for (const auto &grp : agg->groups) {
          AddIfSubqueryAttr(grp.get());
        }
      }
    }
    if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::OrderNode) {
      auto *order =
          dynamic_cast<const ir_sql_converter::SimplestOrderBy *>(node);
      if (order) {
        for (const auto &ord : order->orders) {
          AddIfSubqueryAttr(ord.attr.get());
        }
      }
    }
    for (const auto &child : node->children) {
      CollectPlanAttrs(child.get());
    }
  };
  CollectPlanAttrs(full_ir);

  // (c) Cross-boundary join conditions: one attr in subquery, other outside
  std::function<void(const ir_sql_converter::AQPStmt *)>
      CollectCrossBoundary;
  CollectCrossBoundary = [&](const ir_sql_converter::AQPStmt *node) {
    if (!node)
      return;
    if (node->GetNodeType() == ir_sql_converter::SimplestNodeType::JoinNode) {
      auto *join = dynamic_cast<const ir_sql_converter::SimplestJoin *>(node);
      if (join) {
        for (const auto &cond : join->join_conditions) {
          unsigned int left_table = cond->left_attr->GetTableIndex();
          unsigned int right_table = cond->right_attr->GetTableIndex();
          bool left_in = subquery_tables.count(left_table) > 0;
          bool right_in = subquery_tables.count(right_table) > 0;
          if (left_in && !right_in) {
            AddIfSubqueryAttr(cond->left_attr.get());
          }
          if (right_in && !left_in) {
            AddIfSubqueryAttr(cond->right_attr.get());
          }
        }
      }
    }
    for (const auto &child : node->children) {
      CollectCrossBoundary(child.get());
    }
  };
  CollectCrossBoundary(full_ir);

  return required_attrs;
}

ir_sql_converter::AQPStmt *TopDownSplitter::WrapInProjection(
    ir_sql_converter::AQPStmt *remaining_ir,
    std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>>
        required_attrs) {

  if (!remaining_ir || !found_split_node_) {
    std::cerr << "[TopDownSplitter::WrapInProjection] Error: null input"
              << std::endl;
    return nullptr;
  }

  if (required_attrs.empty()) {
    std::cerr
        << "[TopDownSplitter::WrapInProjection] Warning: no required attrs, "
           "skipping projection wrap"
        << std::endl;
    return nullptr;
  }

  // Find the parent of found_split_node_ and replace the child with a
  // Projection that wraps it.
  std::function<bool(ir_sql_converter::AQPStmt *)> FindAndWrap;
  FindAndWrap = [&](ir_sql_converter::AQPStmt *node) -> bool {
    if (!node)
      return false;
    for (size_t i = 0; i < node->children.size(); i++) {
      if (node->children[i].get() == found_split_node_) {
        // Extract the split node from its parent
        auto split_node = std::move(node->children[i]);

        // Build projection target list (clone required_attrs)
        std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>> proj_tgt;
        for (const auto &attr : required_attrs) {
          proj_tgt.push_back(
              std::make_unique<ir_sql_converter::SimplestAttr>(*attr));
        }

        // Create the projection wrapping the original split node
        std::vector<std::unique_ptr<ir_sql_converter::AQPStmt>>
            proj_children;
        proj_children.push_back(std::move(split_node));
        auto proj_base = std::make_unique<ir_sql_converter::AQPStmt>(
            std::move(proj_children), std::move(proj_tgt),
            ir_sql_converter::SimplestNodeType::ProjectionNode);
        auto projection =
            std::make_unique<ir_sql_converter::SimplestProjection>(
                std::move(proj_base), 0);

        // Put the projection back at the same child slot
        node->children[i] = std::move(projection);

        // Update found_split_node_ to the new projection
        found_split_node_ = node->children[i].get();
        return true;
      }
      if (FindAndWrap(node->children[i].get())) {
        return true;
      }
    }
    return false;
  };

  if (!FindAndWrap(remaining_ir)) {
    std::cerr << "[TopDownSplitter::WrapInProjection] Warning: could not find "
                 "split node in tree; skipping projection wrap"
              << std::endl;
    return nullptr;
  }

  std::cout << "[TopDownSplitter::WrapInProjection] Wrapped split node in "
               "Projection with "
            << required_attrs.size() << " column(s)" << std::endl;
  return found_split_node_;
}

} // namespace middleware
