/*
 * Implementation of foreign key extraction
 */

#include "split/foreign_key_extractor.h"
#include "adapters/duckdb_adapter.h"
#include "adapters/postgres_adapter.h"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include <iostream>
#include <sstream>

namespace middleware {

// ===== ForeignKeyGraph Implementation =====

void ForeignKeyGraph::AddForeignKey(const ForeignKey &fk) {
  fk_from_table_[fk.fk_table].push_back(fk);
  fk_to_table_[fk.pk_table].push_back(fk);
}

std::vector<std::string>
ForeignKeyGraph::GetReferencingTables(const std::string &table) const {
  std::vector<std::string> result;
  auto it = fk_to_table_.find(table);
  if (it != fk_to_table_.end()) {
    for (const auto &fk : it->second) {
      result.push_back(fk.fk_table);
    }
  }
  return result;
}

void ForeignKeyGraph::UpdateForTempTable(
    const std::set<std::string> &executed_table_names,
    const std::string &temp_table_name) {
  // Collect updated FKs (need to rebuild maps since keys change)
  std::vector<ForeignKey> updated_fks;

  for (const auto &[table, fks] : fk_from_table_) {
    for (const auto &fk : fks) {
      bool fk_executed = executed_table_names.count(fk.fk_table) > 0;
      bool pk_executed = executed_table_names.count(fk.pk_table) > 0;

      if (fk_executed && pk_executed) {
        // Both executed: remove (PostgreSQL Prepare4Next line 1553-1555)
        continue;
      }

      ForeignKey updated_fk = fk;
      if (fk_executed) {
        // Redirect con_relid to temp (PostgreSQL line 1559)
        updated_fk.fk_table = temp_table_name;
      }
      if (pk_executed) {
        // Redirect ref_relid to temp (PostgreSQL line 1564)
        updated_fk.pk_table = temp_table_name;
      }
      updated_fks.push_back(std::move(updated_fk));
    }
  }

  // Rebuild maps
  fk_from_table_.clear();
  fk_to_table_.clear();
  for (const auto &fk : updated_fks) {
    AddForeignKey(fk);
  }
}

bool ForeignKeyGraph::HasDirectFK(const std::string &from_table,
                                  const std::string &to_table) const {
  auto it = fk_from_table_.find(from_table);
  if (it != fk_from_table_.end()) {
    for (const auto &fk : it->second) {
      if (fk.pk_table == to_table) {
        return true;
      }
    }
  }
  return false;
}

std::vector<ForeignKey>
ForeignKeyGraph::GetForeignKeys(const std::string &table) const {
  std::vector<ForeignKey> result;

  // Add FKs from this table
  auto from_it = fk_from_table_.find(table);
  if (from_it != fk_from_table_.end()) {
    result.insert(result.end(), from_it->second.begin(), from_it->second.end());
  }

  return result;
}

void ForeignKeyGraph::Print() const {
  std::cout << "=== Foreign Key Graph ===" << std::endl;
  for (const auto &[table, fks] : fk_from_table_) {
    std::cout << "Table: " << table << std::endl;
    for (const auto &fk : fks) {
      std::cout << "  " << fk.fk_table << "." << fk.fk_column << " -> "
                << fk.pk_table << "." << fk.pk_column << std::endl;
    }
  }
  std::cout << "=========================" << std::endl;
}

// ===== ForeignKeyExtractor Implementation =====

ForeignKeyGraph ForeignKeyExtractor::ExtractForTables(
    const std::set<std::string> &table_names) {

#ifndef NDEBUG
  std::cout << "[ForeignKeyExtractor] Extracting foreign keys for "
            << table_names.size() << " table(s)" << std::endl;
#endif

  std::vector<ForeignKey> fks;

  // fixme: now collect foreign ky PG

  //  if (engine_ == BackendEngine::DUCKDB) {
  //    fks = ExtractFromDuckDB(table_names);
  //  } else if (engine_ == BackendEngine::POSTGRESQL) {
  fks = ExtractFromPostgreSQL(table_names);
  //  }

  ForeignKeyGraph graph;
  for (const auto &fk : fks) {
    graph.AddForeignKey(fk);
  }

#ifndef NDEBUG
  std::cout << "[ForeignKeyExtractor] Found " << fks.size()
            << " foreign key relationship(s)" << std::endl;
#endif

  return graph;
}

ForeignKeyGraph ForeignKeyExtractor::ExtractAll() {
#ifndef NDEBUG
  std::cout << "[ForeignKeyExtractor] Extracting all foreign keys from database"
            << std::endl;
#endif

  // Pass empty set to extract all FKs
  return ExtractForTables(std::set<std::string>{});
}

std::vector<ForeignKey> ForeignKeyExtractor::ExtractFromDuckDB(
    const std::set<std::string> &table_names) {

  std::vector<ForeignKey> fks;

  // Use DuckDB's C++ catalog API for direct FK constraint access
  auto *duckdb_adapter = dynamic_cast<DuckDBAdapter *>(adapter_);
  if (!duckdb_adapter) {
    std::cerr << "[ForeignKeyExtractor] Failed to cast adapter to DuckDBAdapter"
              << std::endl;
    return fks;
  }

  auto *context = duckdb_adapter->GetClientContext();
  if (!context) {
    std::cerr << "[ForeignKeyExtractor] No client context available"
              << std::endl;
    return fks;
  }

  try {
    // Begin transaction if in auto-commit mode (required for catalog access)
    if (context->transaction.IsAutoCommit()) {
      context->transaction.BeginTransaction();
    }

    // Get the default catalog and schema
    auto &default_entry =
        context->client_data->catalog_search_path->GetDefault();
    auto &catalog =
        duckdb::Catalog::GetCatalog(*context, default_entry.catalog);

    for (const auto &table_name : table_names) {
      // Get the table catalog entry
      auto table_entry = catalog.GetEntry<duckdb::TableCatalogEntry>(
          *context, default_entry.schema, table_name,
          duckdb::OnEntryNotFound::RETURN_NULL);

      if (!table_entry) {
        continue;
      }

      // Get bound constraints from the table entry
      auto &constraints = table_entry->GetBoundConstraints();
      auto &columns = table_entry->GetColumns();

      for (const auto &constraint : constraints) {
        if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
          continue;
        }

        auto &fk_constraint =
            constraint->Cast<duckdb::BoundForeignKeyConstraint>();
        auto &info = fk_constraint.info;

        // Only process FK_TYPE_FOREIGN_KEY_TABLE entries:
        // this table is the referencing (FK) table, info.table is the
        // referenced (PK) table
        if (info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
          continue;
        }

        // Get the referenced (PK) table entry for column name resolution
        auto pk_table_entry = catalog.GetEntry<duckdb::TableCatalogEntry>(
            *context, default_entry.schema, info.table,
            duckdb::OnEntryNotFound::RETURN_NULL);
        if (!pk_table_entry) {
          continue;
        }
        auto &pk_columns = pk_table_entry->GetColumns();

        // Extract each FK column pair
        for (size_t i = 0; i < info.fk_keys.size(); i++) {
          auto &fk_col = columns.GetColumn(info.fk_keys[i]);
          auto &pk_col = pk_columns.GetColumn(info.pk_keys[i]);

          fks.emplace_back(table_name, fk_col.GetName(), info.table,
                           pk_col.GetName());
        }
      }
    }

    // Commit transaction if we started one
    if (context->transaction.IsAutoCommit()) {
      context->transaction.Commit();
    }
  } catch (const std::exception &e) {
    std::cerr << "[ForeignKeyExtractor] Error extracting FKs from DuckDB API: "
              << e.what() << std::endl;
  }

  return fks;
}

std::vector<ForeignKey> ForeignKeyExtractor::ExtractFromPostgreSQL(
    const std::set<std::string> &table_names) {

  std::vector<ForeignKey> fks;

  // PostgreSQL stores FK constraints in information_schema
  std::string query = R"(
   SELECT
     tc.table_name as fk_table,
     kcu.column_name as fk_column,
     ccu.table_name AS pk_table,
     ccu.column_name AS pk_column
   FROM information_schema.table_constraints AS tc
   JOIN information_schema.key_column_usage AS kcu
     ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
   JOIN information_schema.constraint_column_usage AS ccu
     ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema = tc.table_schema
   WHERE tc.constraint_type = 'FOREIGN KEY'
 )";

  // Add table filter if specific tables requested
  if (!table_names.empty()) {
    std::ostringstream table_list;
    bool first = true;
    for (const auto &table : table_names) {
      if (!first)
        table_list << ", ";
      table_list << "'" << table << "'";
      first = false;
    }
    query += " AND tc.table_name IN (" + table_list.str() + ")";
  }

  try {
    // fixme: now collect foreign ky PG
    auto postgres_adapter = std::make_unique<PostgreSQLAdapter>(
        "host=localhost port=5432 dbname=imdb user=imdb");
    auto result = postgres_adapter->ExecuteSQL(query);

    for (const auto &row : result.rows) {
      if (row.size() >= 4) {
        fks.emplace_back(row[0], row[1], row[2], row[3]);
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "[ForeignKeyExtractor] Error extracting FKs from PostgreSQL: "
              << e.what() << std::endl;
  }

  return fks;
}

} // namespace middleware
