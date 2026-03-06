#include "adapters/opengauss_adapter.h"

namespace middleware {
OpenGaussAdapter::OpenGaussAdapter(const std::string &connection_string)
    : PostgreSQLAdapter(connection_string) {}
} // namespace middleware
