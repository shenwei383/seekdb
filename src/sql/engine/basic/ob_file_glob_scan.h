/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include "sql/engine/basic/ob_file_csv_scan.h"
#include <algorithm>
#include <glob.h>
#include <string>
#include <vector>

namespace oceanbase {
namespace sql {

class FileGlobScan {
public:
  enum class Status { OK, INVALID, DENIED, IO_ERROR };

  Status open(const std::string &pattern, const std::string &secure_root)
  {
    files_.clear();
    index_ = 0;
    if (pattern.empty() || pattern.find('\0') != std::string::npos ||
        pattern.size() > PATH_MAX || secure_root.empty()) return Status::INVALID;
    const size_t slash = pattern.rfind('/');
    if (slash == std::string::npos || slash + 1 == pattern.size()) return Status::INVALID;
    const std::string basename = pattern.substr(slash + 1);
    const std::string parent = slash == 0 ? "/" : pattern.substr(0, slash);
    char canonical_parent[PATH_MAX + 1];
    char canonical_root[PATH_MAX + 1];
    if (!realpath(parent.c_str(), canonical_parent)) return Status::IO_ERROR;
    if (!realpath(secure_root.c_str(), canonical_root)) return Status::DENIED;
    const std::string root(canonical_root), directory(canonical_parent);
    if (root != "/" && directory != root &&
        (directory.size() <= root.size() || directory.compare(0, root.size(), root) != 0 ||
         directory[root.size()] != '/')) return Status::DENIED;
    const std::string checked_pattern = directory == "/" ? "/" + basename :
        directory + "/" + basename;
    glob_t matches = {};
    const int outcome = ::glob(checked_pattern.c_str(), GLOB_NOSORT | GLOB_ERR, nullptr, &matches);
    if (outcome == GLOB_NOMATCH) {
      ::globfree(&matches);
      return Status::OK;
    }
    if (outcome != 0 || matches.gl_pathc > 100000) {
      ::globfree(&matches);
      return Status::IO_ERROR;
    }
    for (size_t i = 0; i < matches.gl_pathc; ++i) {
      const std::string candidate(matches.gl_pathv[i]);
      struct stat st;
      if (::lstat(candidate.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
      const int fd = open_secure_csv_fd(root, candidate);
      if (fd < 0) {
        ::globfree(&matches);
        files_.clear();
        return Status::DENIED;
      }
      ::close(fd);
      files_.push_back(candidate);
    }
    ::globfree(&matches);
    std::sort(files_.begin(), files_.end());
    return Status::OK;
  }

  bool next(std::string &path)
  {
    if (index_ >= files_.size()) return false;
    path = files_[index_++];
    return true;
  }

  void rewind() { index_ = 0; }
  void close() { files_.clear(); index_ = 0; }

private:
  std::vector<std::string> files_;
  size_t index_ = 0;
};

} // namespace sql
} // namespace oceanbase
