#include "util.h"

namespace middleware {

// Helper function to check if a file ends with .sql
bool ends_with_sql(const std::string &filename) {
  if (filename.length() < 4)
    return false;
  return filename.substr(filename.length() - 4) == ".sql";
}

// Helper function to get all .sql files from a directory
std::vector<std::string> get_sql_files(const std::string &directory) {
  std::vector<std::string> sql_files;

  DIR *dir = opendir(directory.c_str());
  if (!dir) {
    throw std::runtime_error("Cannot open directory: " + directory);
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    std::string filename = entry->d_name;

    // Skip . and ..
    if (filename == "." || filename == "..") {
      continue;
    }

    // Check if it's a .sql file
    if (ends_with_sql(filename)) {
      std::string full_path = directory;
      if (full_path.back() != '/') {
        full_path += '/';
      }
      full_path += filename;
      sql_files.push_back(full_path);
    }
  }

  closedir(dir);
  return sql_files;
}

// Helper function to extract filename from path
std::string get_filename(const std::string &path) {
  size_t last_slash = path.find_last_of('/');
  if (last_slash != std::string::npos) {
    return path.substr(last_slash + 1);
  }
  return path;
}
}
