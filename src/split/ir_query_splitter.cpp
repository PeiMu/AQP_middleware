/*
 * IR-level query splitter main pipeline
 */

#include "split/ir_query_splitter.h"

#ifdef HAVE_DUCKDB
#include "adapters/duckdb_adapter.h"
#endif

namespace middleware {

IRQuerySplitter::IRQuerySplitter(DBAdapter *adapter, const ParamConfig &config)
    : adapter_(adapter), config_(config) {

  std::cout << "[IRQuerySplitter] Initializing with strategy: "
            << config.GetStrategyName() << std::endl;

  // Create the appropriate splitter based on strategy
  switch (config.strategy) {
  case SplitStrategy::TOP_DOWN:
    splitter_ =
        std::make_unique<TopDownSplitter>(adapter, config.enable_reorder_get);
    break;

  case SplitStrategy::MIN_SUBQUERY:
    splitter_ = std::make_unique<MinSubquerySplitter>(adapter, config.engine);
    std::cout << "[IRQuerySplitter] Created MinSubquery splitter" << std::endl;
    break;

  case SplitStrategy::RELATIONSHIP_CENTER:
    splitter_ =
        std::make_unique<RelationshipCenterSplitter>(adapter, config.engine);
    std::cout << "[IRQuerySplitter] Created RelationshipCenter splitter"
              << std::endl;
    break;

  case SplitStrategy::ENTITY_CENTER:
    splitter_ = std::make_unique<EntityCenterSplitter>(adapter, config.engine);
    std::cout << "[IRQuerySplitter] Created EntityCenter splitter" << std::endl;
    break;

  case SplitStrategy::NONE:
  default:
    splitter_ = nullptr;
    break;
  }
}

QueryResult IRQuerySplitter::ExecuteWithSplit(const std::string &sql) {
  std::cout
      << "\n[IRQuerySplitter] ========== Starting Split Execution =========="
      << std::endl;

  if (!config_.NeedsSplit() || !splitter_) {
    std::cout << "[IRQuerySplitter] No splitting needed, executing directly"
              << std::endl;
    return adapter_->ExecuteSQL(sql);
  }

  // === Phase 1: Parse SQL ===
  std::cout << "[IRQuerySplitter] Phase 1: Parsing SQL" << std::endl;
  adapter_->ParseSQL(sql);

  // === Phase 2: Pre-Optimize (ONLY for DuckDB) ===
  // PreOptimize runs JOIN_ORDER optimizer which sets cardinality
  if (config_.engine == BackendEngine::DUCKDB &&
      config_.strategy == SplitStrategy::TOP_DOWN) {
    std::cout << "[IRQuerySplitter] Phase 2: Pre-Optimization (DuckDB)"
              << std::endl;
#ifdef HAVE_DUCKDB
    auto *duckdb_adapter = dynamic_cast<DuckDBAdapter *>(adapter_);
    if (duckdb_adapter) {
      duckdb_adapter->PreOptimizePlan();
      std::cout << "[IRQuerySplitter] Phase 2: After Pre-Optimization (DuckDB)"
                << std::endl;
      duckdb_adapter->PrintLogicalPlan();
    }
#endif
  } else {
    std::cout << "[IRQuerySplitter] Phase 2: Skipping Pre-Optimization "
                 "(PostgreSQL doesn't need it)"
              << std::endl;
  }

  // === Phase 3: Convert to IR ===
  std::cout << "[IRQuerySplitter] Phase 3: Converting to IR" << std::endl;
  auto whole_ir = adapter_->ConvertPlanToIR();

  if (!whole_ir) {
    throw std::runtime_error("Failed to convert plan to IR");
  }

  if (config_.enable_debug_print) {
    std::cout << "\n=== Whole IR (before split) ===" << std::endl;
    whole_ir->Print();
  }

  // === Phase 4: Iterative Split-Execute Loop ===
  std::cout << "[IRQuerySplitter] Phase 4: Iterative Split-Execute Loop"
            << std::endl;
  auto result = ExecuteSplitLoop(std::move(whole_ir));

  std::cout
      << "[IRQuerySplitter] ========== Split Execution Complete =========="
      << std::endl;
  std::cout << "Total iterations: " << iteration_count_ << std::endl;

  return result;
}

QueryResult IRQuerySplitter::ExecuteSplitLoop(
    std::unique_ptr<ir_sql_converter::SimplestStmt> whole_ir) {

  iteration_count_ = 0;
  std::unique_ptr<ir_sql_converter::SimplestStmt> remaining_ir =
      std::move(whole_ir);

  // === Strategy Preprocessing (called ONCE before the loop) ===
  // This builds the join graph, extracts foreign keys, marks
  // entity/relationship
  std::cout << "[IRQuerySplitter] Strategy Preprocessing" << std::endl;
  splitter_->Preprocess(remaining_ir);

  // Main loop: while (graph has edges) { extract → execute → merge }
  // Pattern from PostgreSQL: query_split.c QSExecutor loop
  // Exit condition: IsComplete() returns true when join graph has no remaining
  // edges
  while (!splitter_->IsComplete(remaining_ir.get())) {
    iteration_count_++;

    std::cout << "\n========== Iteration " << iteration_count_
              << " ==========" << std::endl;

    auto iter_start = std::chrono::high_resolution_clock::now();

    // Execute one iteration (extract subquery, execute, update IR)
    // Returns false only if ExtractNextSubquery fails unexpectedly
    if (!ExecuteOneIteration(remaining_ir)) {
      std::cerr << "[IRQuerySplitter] Warning: ExecuteOneIteration returned "
                   "false but IsComplete was false. Breaking loop."
                << std::endl;
      break;
    }

    auto iter_end = std::chrono::high_resolution_clock::now();
    double iter_time_ms =
        std::chrono::duration<double, std::milli>(iter_end - iter_start)
            .count();
    iteration_times_.push_back(iter_time_ms);

    std::cout << "Iteration " << iteration_count_ << " completed in "
              << iter_time_ms << " ms" << std::endl;
  }

  std::cout << "[IRQuerySplitter] Split loop completed after "
            << iteration_count_ << " iteration(s)" << std::endl;

  // === Final Execution ===
  if (!remaining_ir) {
    throw std::runtime_error("Remaining IR is null after split loop");
  }

  if (config_.enable_debug_print) {
    std::cout << "\n=== Final Remaining IR ===" << std::endl;
    remaining_ir->Print();
  }

  // Check if remaining IR is trivial (just a temp table reference)
  // If so, we can return the temp table contents directly without re-executing
  std::string trivial_temp = GetTrivialTempTable(remaining_ir.get());
  if (!trivial_temp.empty()) {
    std::cout << "\n[IRQuerySplitter] Final IR is trivial (temp table: "
              << trivial_temp << "), returning directly" << std::endl;

    // Just SELECT * FROM the temp table
    std::string final_sql = "SELECT * FROM " + trivial_temp;
    if (config_.enable_debug_print) {
      std::cout << "=== Final SQL (trivial) ===" << std::endl;
      std::cout << final_sql << std::endl;
    }
    return adapter_->ExecuteSQL(final_sql);
  }

  // Non-trivial case: generate and execute final SQL
  std::cout << "\n[IRQuerySplitter] Executing final remaining IR" << std::endl;

  std::string final_sql =
      adapter_->GenerateSQL(*remaining_ir, adapter_->subquery_index++);

  if (config_.enable_debug_print) {
    std::cout << "\n=== Final Generated SQL ===" << std::endl;
    std::cout << final_sql << std::endl;
  }

  return adapter_->ExecuteSQL(final_sql);
}

bool IRQuerySplitter::ExecuteOneIteration(
    std::unique_ptr<ir_sql_converter::SimplestStmt> &remaining_ir) {

  // === Step 1: Extract Next Subquery ===
  std::cout << "[Iteration " << iteration_count_
            << "] Step 1: Extracting next subquery" << std::endl;

  auto extraction = splitter_->ExtractNextSubquery(remaining_ir.get());

  if (!extraction) {
    std::cout << "[Iteration " << iteration_count_
              << "] No more subqueries to extract" << std::endl;
    return false;
  }

  std::cout << "[Iteration " << iteration_count_ << "] Extracted subquery with "
            << extraction->executed_table_indices.size() << " table(s)"
            << std::endl;

  // === Step 2: Execute Sub-IR ===
  std::cout << "[Iteration " << iteration_count_ << "] Step 2: Executing sub-IR"
            << std::endl;

  // Get the executable IR (prefers built sub_ir over pipeline_breaker_ptr)
  ir_sql_converter::SimplestStmt *executable_ir = extraction->GetExecutableIR();

  if (!executable_ir) {
    std::cerr << "[Iteration " << iteration_count_
              << "] Error: No executable IR (sub_ir or pipeline_breaker_ptr)"
              << std::endl;
    return false;
  }

  if (extraction->sub_ir) {
    std::cout << "[Iteration " << iteration_count_
              << "] Using built sub-IR for execution" << std::endl;
  } else {
    std::cout << "[Iteration " << iteration_count_
              << "] Using pipeline_breaker_ptr for execution" << std::endl;
  }

  if (config_.enable_debug_print) {
    std::cout << "\n=== Sub-IR to Execute ===" << std::endl;
    executable_ir->Print();
  }

  // Generate SQL from the sub-IR
  std::string sub_sql =
      adapter_->GenerateSQL(*executable_ir, adapter_->subquery_index++);
  std::string temp_table_name = GenerateTempTableName();

  if (config_.enable_debug_print) {
    std::cout << "\n=== Sub-Query SQL ===" << std::endl;
    std::cout << sub_sql << std::endl;
  }

  std::cout << "Executing sub-query and creating temp table: "
            << temp_table_name << std::endl;

  adapter_->ExecuteSQLandCreateTempTable(sub_sql, temp_table_name);

  unsigned int temp_table_index = adapter_->subquery_index - 1;
  uint64_t cardinality = adapter_->GetTempTableCardinality(temp_table_name);

  TempTableInfo temp_table =
      TempTableInfo(temp_table_name, temp_table_index, cardinality);

  // Store column names from the executed node
  for (const auto &attr : executable_ir->target_list) {
    temp_table.column_names.push_back(attr->GetColumnName());
  }

  std::cout << "[Iteration " << iteration_count_
            << "] Created temp table: " << temp_table.table_name
            << " (cardinality=" << temp_table.cardinality << ")" << std::endl;

  // === Step 3: Update Remaining IR ===
  std::cout << "[Iteration " << iteration_count_
            << "] Step 3: Updating remaining IR" << std::endl;

  // Note: For UpdateRemainingIR, we need to find the node in remaining_ir to
  // replace If we built a new sub_ir, we need to find where to replace in the
  // original tree For now, use pipeline_breaker_ptr if available, otherwise
  // skip update
  ir_sql_converter::SimplestStmt *node_to_replace =
      extraction->pipeline_breaker_ptr;

  if (!node_to_replace && extraction->sub_ir) {
    // We built a new sub-IR, need to find the corresponding node in
    // remaining_ir For now, we'll try to find it using FindSubIRForCluster
    // logic This is a simplification - in production, we'd track the original
    // node
    std::cout << "[Iteration " << iteration_count_
              << "] Note: Using built sub-IR, searching for node to replace"
              << std::endl;
  }

  if (node_to_replace) {
    UpdateRemainingIR(remaining_ir, temp_table, node_to_replace,
                      extraction->executed_table_indices);
  } else {
    std::cerr << "[Iteration " << iteration_count_
              << "] Warning: No node to replace in remaining IR" << std::endl;
  }

  if (config_.enable_debug_print) {
    std::cout << "\n=== Updated Remaining IR ===" << std::endl;
    remaining_ir->Print();
  }

  temp_tables_.push_back(temp_table);

  return true;
}

TempTableInfo IRQuerySplitter::ExecuteSubIR(
    std::unique_ptr<ir_sql_converter::SimplestStmt> sub_ir,
    const std::set<unsigned int> &executed_table_indices) {

  // Generate temp table name (uses adapter's counter)
  std::string temp_table_name = GenerateTempTableName();

  // Convert sub-IR to SQL using adapter_->GenerateSQL()
  std::string sub_sql =
      adapter_->GenerateSQL(*sub_ir, adapter_->subquery_index++);

  if (config_.enable_debug_print) {
    std::cout << "\n=== Sub-Query SQL ===" << std::endl;
    std::cout << sub_sql << std::endl;
  }

  // Execute and create temp table
  // Uses: adapter_->ExecuteSQLandCreateTempTable()
  std::cout << "Executing sub-query and creating temp table: "
            << temp_table_name << std::endl;

  adapter_->ExecuteSQLandCreateTempTable(sub_sql, temp_table_name);

  // For now, use the temp table index from adapter
  unsigned int temp_table_index = adapter_->subquery_index - 1;
  uint64_t cardinality = adapter_->GetTempTableCardinality(temp_table_name);

  return TempTableInfo(temp_table_name, temp_table_index, cardinality);
}

void IRQuerySplitter::UpdateRemainingIR(
    std::unique_ptr<ir_sql_converter::SimplestStmt> &remaining_ir,
    const TempTableInfo &temp_table,
    ir_sql_converter::SimplestStmt *node_to_replace,
    const std::set<unsigned int> &old_table_indices) {

  std::cout << "[UpdateRemainingIR] Replacing node with temp table: "
            << temp_table.table_name << std::endl;

  if (!remaining_ir || !node_to_replace) {
    std::cerr << "[UpdateRemainingIR] Error: null pointer" << std::endl;
    return;
  }

  // Strategy: Find the parent of node_to_replace and replace the child pointer
  bool replaced = ReplaceNodeWithTempTable(remaining_ir.get(), node_to_replace,
                                           temp_table, old_table_indices);

  if (replaced) {
    std::cout << "[UpdateRemainingIR] Successfully replaced node" << std::endl;
  } else {
    std::cerr << "[UpdateRemainingIR] Warning: Failed to find and replace node"
              << std::endl;
  }
}

// Helper function to find and replace a node in the IR tree
bool IRQuerySplitter::ReplaceNodeWithTempTable(
    ir_sql_converter::SimplestStmt *current,
    ir_sql_converter::SimplestStmt *node_to_replace,
    const TempTableInfo &temp_table,
    const std::set<unsigned int> &old_table_indices) {

  if (!current) {
    return false;
  }

  // Check each child
  for (size_t i = 0; i < current->children.size(); i++) {
    auto &child = current->children[i];

    if (!child) {
      continue;
    }

    // If this child is the node we want to replace
    if (child.get() == node_to_replace) {
      std::cout << "[ReplaceNodeWithTempTable] Found target node at child[" << i
                << "]" << std::endl;

      // Create a new SimplestChunk node to represent the temp table
      std::vector<std::string> chunk_contents;
      chunk_contents.push_back(temp_table.table_name);

      std::vector<std::unique_ptr<ir_sql_converter::SimplestStmt>>
          empty_children;
      std::vector<std::unique_ptr<ir_sql_converter::SimplestAttr>> empty_attrs;
      auto base_stmt = std::make_unique<ir_sql_converter::SimplestStmt>(
          std::move(empty_children), std::move(empty_attrs),
          ir_sql_converter::SimplestNodeType::StmtNode);
      auto temp_table_node = std::make_unique<ir_sql_converter::SimplestChunk>(
          std::move(base_stmt), temp_table.table_index, chunk_contents);

      // Copy the target_list from the original node
      for (const auto &attr : child->target_list) {
        auto cloned_attr =
            std::make_unique<ir_sql_converter::SimplestAttr>(*attr);
        temp_table_node->target_list.push_back(std::move(cloned_attr));
      }

      std::cout << "[ReplaceNodeWithTempTable] Created SimplestChunk with "
                << temp_table_node->target_list.size() << " columns"
                << std::endl;

      // Replace the child
      current->children[i] = std::move(temp_table_node);

      std::cout << "[ReplaceNodeWithTempTable] Replacement successful"
                << std::endl;
      return true;
    }

    // Recursively search in this child's subtree
    if (ReplaceNodeWithTempTable(child.get(), node_to_replace, temp_table,
                                 old_table_indices)) {
      return true;
    }
  }

  return false;
}

std::string IRQuerySplitter::GenerateTempTableName() {
  return "temp" + std::to_string(adapter_->subquery_index);
}

void IRQuerySplitter::PrintIterationInfo(int iteration,
                                         const std::string &info) {
  if (config_.enable_debug_print) {
    std::cout << "[Iteration " << iteration << "] " << info << std::endl;
  }
}

std::string
IRQuerySplitter::GetTrivialTempTable(ir_sql_converter::SimplestStmt *ir) const {
  if (!ir) {
    return "";
  }

  // Case 1: IR is directly a ChunkNode (temp table reference)
  if (ir->GetNodeType() == ir_sql_converter::SimplestNodeType::ChunkNode) {
    auto *chunk = dynamic_cast<ir_sql_converter::SimplestChunk *>(ir);
    if (chunk && !chunk->GetContents().empty()) {
      return chunk->GetContents()[0]; // Return temp table name
    }
  }

  // Case 2: IR is a Projection over a single ChunkNode
  if (ir->GetNodeType() == ir_sql_converter::SimplestNodeType::ProjectionNode) {
    if (ir->children.size() == 1 && ir->children[0]) {
      auto *child = ir->children[0].get();
      if (child->GetNodeType() ==
          ir_sql_converter::SimplestNodeType::ChunkNode) {
        auto *chunk = dynamic_cast<ir_sql_converter::SimplestChunk *>(child);
        if (chunk && !chunk->GetContents().empty()) {
          return chunk->GetContents()[0];
        }
      }
    }
  }

  // Case 3: Check if there's only one node and it's a ChunkNode
  // (remaining_ir might be wrapped in a generic StmtNode)
  if (ir->children.size() == 1 && ir->children[0]) {
    return GetTrivialTempTable(ir->children[0].get());
  }

  // Not trivial - has actual operations to perform
  return "";
}

} // namespace middleware
