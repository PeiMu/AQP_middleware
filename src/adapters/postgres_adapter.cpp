/*
 * PostgreSQL adapter for binding IR to the PostgreSQL engine
 * */

#include "postgres_adapter.h"

namespace middleware {

PostgreSQLAdapter::PostgreSQLAdapter(const std::string &connection_string)
    : conn(nullptr), parse_tree() {
  // Connect to PostgreSQL
  conn = PQconnectdb(connection_string.c_str());

  if (CONNECTION_OK != PQstatus(conn)) {
    std::string error_msg = PQerrorMessage(conn);
    PQfinish(conn);
    conn = nullptr;
    throw std::runtime_error("PostgreSQL connection failed: " + error_msg);
  }

  std::cout << "[PostgreSQL] Connected to database: " << PQdb(conn) << "@"
            << PQhost(conn) << std::endl;
}

PostgreSQLAdapter::~PostgreSQLAdapter() { CleanUp(); }

void PostgreSQLAdapter::ParseSQL(const std::string &sql) {
  CheckConnection();

  // Parse SQL using libpg_query
  PgQueryParseResult result = pg_query_parse(sql.c_str());

  if (result.error) {
    std::string error_msg =
        "Parse error: " + std::string(result.error->message);
    pg_query_free_parse_result(result);
    throw std::runtime_error(error_msg);
  }

  // Parse JSON
  parse_tree = json::parse(result.parse_tree);
  pg_query_free_parse_result(result);
}

std::unique_ptr<ir_sql_converter::SimplestStmt>
PostgreSQLAdapter::ConvertPlanToIR() {
  if (parse_tree.empty()) {
    throw std::runtime_error("No parse tree available. Call ParseSQL first.");
  }

  std::unique_ptr<ir_sql_converter::SimplestStmt> stmt =
      ir_sql_converter::ConvertParseTreeToIR(parse_tree, subquery_index);
  return std::move(stmt);
}

QueryResult PostgreSQLAdapter::ExecuteSQL(const std::string &sql) {
  CheckConnection();

  QueryResult result;

  // Execute query
  PGresult *pg_result = PQexec(conn, sql.c_str());

  // Check for errors
  ExecStatusType status = PQresultStatus(pg_result);
  if (PGRES_TUPLES_OK != status && PGRES_COMMAND_OK != status) {
    std::string error_msg =
        "Query execution failed: " + std::string(PQerrorMessage(conn));
    PQclear(pg_result);
    throw std::runtime_error(error_msg);
  }

  // Get column information
  result.num_columns = PQnfields(pg_result);
  for (int i = 0; i < result.num_columns; i++) {
    result.column_names.push_back(PQfname(pg_result, i));
  }

  // Get row data
  result.num_rows = PQntuples(pg_result);
  result.rows.reserve(result.num_rows);

  for (int row = 0; row < result.num_rows; row++) {
    std::vector<std::string> row_data;
    row_data.reserve(result.num_columns);

    for (int col = 0; col < result.num_columns; col++) {
      // Check if value is NULL
      if (PQgetisnull(pg_result, row, col)) {
        row_data.push_back("NULL");
      } else {
        row_data.push_back(PQgetvalue(pg_result, row, col));
      }
    }

    result.rows.push_back(std::move(row_data));
  }

  PQclear(pg_result);
  return result;
}

void PostgreSQLAdapter::ExecuteSQLandCreateTempTable(const std::string &sql) {}

void PostgreSQLAdapter::CreateTempTable(const std::string &table_name,
                                        const QueryResult &result) {}

void PostgreSQLAdapter::DropTempTable(const std::string &table_name) {
  CheckConnection();

  std::string drop_sql = "DROP TABLE IF EXISTS " + table_name;
  PGresult *pg_result = PQexec(conn, drop_sql.c_str());

  if (PQresultStatus(pg_result) != PGRES_COMMAND_OK) {
    std::string error_msg =
        "Failed to drop temp table: " + std::string(PQerrorMessage(conn));
    PQclear(pg_result);
    throw std::runtime_error(error_msg);
  }

  PQclear(pg_result);

  std::cout << "[PostgreSQL] Dropped temp table: " << table_name << std::endl;
}

bool PostgreSQLAdapter::TempTableExists(const std::string &table_name) {
  CheckConnection();

  std::string check_sql = "SELECT EXISTS ("
                          "  SELECT 1 FROM pg_tables "
                          "  WHERE tablename = '" +
                          table_name +
                          "' "
                          "  AND schemaname = 'pg_temp_" +
                          std::to_string(PQbackendPID(conn)) +
                          "'"
                          ")";

  PGresult *pg_result = PQexec(conn, check_sql.c_str());

  bool exists = false;
  if (PQresultStatus(pg_result) == PGRES_TUPLES_OK &&
      PQntuples(pg_result) > 0) {
    exists = (PQgetvalue(pg_result, 0, 0)[0] == 't');
  }

  PQclear(pg_result);
  return exists;
}

void PostgreSQLAdapter::CleanUp() {
  parse_tree.clear();

  // Close connection
  if (conn) {
    PQfinish(conn);
    conn = nullptr;
    std::cout << "[PostgreSQL] Connection closed" << std::endl;
  }
}

void PostgreSQLAdapter::CheckConnection() {
  if (!conn || CONNECTION_OK != PQstatus(conn)) {
    throw std::runtime_error("PostgreSQL connection is not valid");
  }
}
} // namespace middleware