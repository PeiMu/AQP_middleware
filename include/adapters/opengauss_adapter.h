#pragma once
#include "adapters/postgres_adapter.h"

namespace middleware {
class OpenGaussAdapter : public PostgreSQLAdapter {
public:
  explicit OpenGaussAdapter(const std::string &connection_string);
  ~OpenGaussAdapter() override = default;
  std::string GetEngineName() const override { return "OpenGauss"; }
};
} // namespace middleware
