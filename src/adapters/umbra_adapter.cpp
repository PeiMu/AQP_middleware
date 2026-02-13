#include "adapters/umbra_adapter.h"
#include <iostream>

namespace middleware {

UmbraAdapter::UmbraAdapter(const std::string &connection_string)
    : PostgreSQLAdapter(connection_string) {}

void UmbraAdapter::SetTempTableCardinality(const std::string &temp_table_name,
                                           uint64_t cardinality) {
  // Try the same UPDATE pg_class approach as PostgreSQL.
  // Umbra may not expose pg_class, so treat failure as non-fatal.
  std::string update_sql =
      "UPDATE pg_class SET reltuples = " + std::to_string(cardinality) +
      " WHERE relname = '" + temp_table_name + "'";
  PGresult *pg_result = PQexec(GetConnection(), update_sql.c_str());

  if (PQresultStatus(pg_result) != PGRES_COMMAND_OK) {
#ifndef NDEBUG
    std::cerr << "[Umbra] Warning: Failed to set reltuples (non-fatal): "
              << PQerrorMessage(GetConnection()) << std::endl;
#endif
  }

  PQclear(pg_result);

#ifndef NDEBUG
  std::cout << "[Umbra] SetTempTableCardinality: " << temp_table_name << " = "
            << cardinality << std::endl;
#endif
}

} // namespace middleware
