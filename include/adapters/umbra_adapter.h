#pragma once
#include "adapters/postgres_adapter.h"

namespace middleware {
class UmbraAdapter : public PostgreSQLAdapter {
public:
  explicit UmbraAdapter(const std::string &connection_string);
  ~UmbraAdapter() override = default;

  std::string GetEngineName() const override { return "Umbra"; }

  void SetTempTableCardinality(const std::string &temp_table_name,
                               uint64_t cardinality) override;
};
} // namespace middleware
