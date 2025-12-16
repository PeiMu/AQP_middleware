/*
* Utility functions
* */

#pragma once

#include <fstream>
#include <sstream>
#include <dirent.h>
#include <sys/stat.h>
#include <cstring>
#include <vector>

//#define DEBUG

namespace middleware {
bool ends_with_sql(const std::string &filename);
std::vector<std::string> get_sql_files(const std::string &directory);
std::string get_filename(const std::string &path);
}
