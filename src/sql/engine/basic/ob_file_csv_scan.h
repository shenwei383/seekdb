/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <cstdio>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/openat2.h>
#include <sys/syscall.h>
#endif

namespace oceanbase {
namespace sql {

// Open the checked canonical path relative to a pinned secure_file_priv directory.
// The kernel resolves every component beneath that directory in one operation,
// preventing symlink replacement between a path check and fopen().
inline int open_secure_csv_fd(const std::string &secure_root, const std::string &canonical_file)
{
#ifdef __linux__
  char root_path[PATH_MAX + 1];
  if (secure_root.empty() || realpath(secure_root.c_str(), root_path) == nullptr) {
    errno = EACCES;
    return -1;
  }
  const std::string root(root_path);
  const size_t prefix_len = root == "/" ? 1 : root.size() + 1;
  if (canonical_file.size() <= prefix_len ||
      (root != "/" && (canonical_file.compare(0, root.size(), root) != 0 ||
                       canonical_file[root.size()] != '/')) ||
      (root == "/" && canonical_file[0] != '/')) {
    errno = EACCES;
    return -1;
  }
  const int slash_fd = ::open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (slash_fd < 0) return -1;
  int root_fd = slash_fd;
  if (root != "/") {
    struct open_how root_how = {};
    root_how.flags = O_PATH | O_DIRECTORY | O_CLOEXEC;
    root_how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;
    root_fd = static_cast<int>(::syscall(SYS_openat2, slash_fd, root.c_str() + 1,
                                          &root_how, sizeof(root_how)));
    const int saved_errno = errno;
    ::close(slash_fd);
    errno = saved_errno;
    if (root_fd < 0) return -1;
  }
  struct open_how file_how = {};
  file_how.flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
  file_how.resolve = RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS;
  const int fd = static_cast<int>(::syscall(SYS_openat2, root_fd,
      canonical_file.c_str() + prefix_len, &file_how, sizeof(file_how)));
  const int saved_errno = errno;
  ::close(root_fd);
  errno = saved_errno;
  if (fd >= 0) {
    struct stat st;
    const int stat_result = ::fstat(fd, &st);
    if (stat_result != 0 || !S_ISREG(st.st_mode)) {
      const int status_errno = stat_result != 0 ? errno : EACCES;
      ::close(fd);
      errno = status_errno;
      return -1;
    }
  }
  return fd;
#else
  (void)secure_root;
  (void)canonical_file;
  errno = ENOTSUP;
  return -1;
#endif
}

// A bounded-record CSV reader. Rows are consumed from disk on demand.
class FileCsvScan {
public:
  enum class Status { OK, END, INVALID, IO_ERROR, CHANGED };
  static constexpr size_t MAX_FIELD_BYTES = 16 * 1024 * 1024;
  FileCsvScan() : file_(nullptr), delimiter_(','), row_number_(0) {}
  ~FileCsvScan() { close(); }
  FileCsvScan(const FileCsvScan &) = delete;
  FileCsvScan &operator=(const FileCsvScan &) = delete;

  Status open(const std::string &path, char delimiter = ',') {
    close();
    if (path.empty() || delimiter == '"' || delimiter == '\r' || delimiter == '\n') return Status::INVALID;
    file_ = std::fopen(path.c_str(), "rb");
    if (nullptr == file_) return Status::IO_ERROR;
    delimiter_ = delimiter;
    row_number_ = 0;
    if (0 != fstat(fileno(file_), &initial_stat_) || !S_ISREG(initial_stat_.st_mode)) {
      close();
      return Status::IO_ERROR;
    }
    return Status::OK;
  }
  // Takes ownership of fd on both success and failure.
  Status open_fd(int fd) {
    close();
    if (fd < 0) return Status::IO_ERROR;
    file_ = ::fdopen(fd, "rb");
    if (file_ == nullptr) {
      ::close(fd);
      return Status::IO_ERROR;
    }
    delimiter_ = ',';
    row_number_ = 0;
    if (0 != fstat(fileno(file_), &initial_stat_) || !S_ISREG(initial_stat_.st_mode)) {
      close();
      return Status::IO_ERROR;
    }
    return Status::OK;
  }
  Status rewind() {
    if (nullptr == file_) return Status::IO_ERROR;
    if (Status::OK != check_unchanged()) return Status::CHANGED;
    if (0 != std::fseek(file_, 0, SEEK_SET)) return Status::IO_ERROR;
    row_number_ = 0;
    return Status::OK;
  }
  void close() {
    if (nullptr != file_) { std::fclose(file_); file_ = nullptr; }
    row_number_ = 0;
  }
  size_t row_number() const { return row_number_; }
  const struct stat &initial_stat() const { return initial_stat_; }

  Status next(std::vector<std::string> &row) {
    if (nullptr == file_) return Status::IO_ERROR;
    if (Status::OK != check_unchanged()) return Status::CHANGED;
    row.clear();
    std::string field;
    enum State { FIELD_START, UNQUOTED, QUOTED, AFTER_QUOTE } state = FIELD_START;
    bool any = false;
    for (;;) {
      const int ch = std::fgetc(file_);
      if (EOF == ch) {
        if (std::ferror(file_)) return Status::IO_ERROR;
        if (!any) return Status::END;
        if (QUOTED == state) return Status::INVALID;
        row.push_back(field); ++row_number_; return check_unchanged();
      }
      any = true;
      if (field.size() > MAX_FIELD_BYTES) return Status::INVALID;
      if (FIELD_START == state) {
        if ('"' == ch) state = QUOTED;
        else if (delimiter_ == ch) row.emplace_back();
        else if ('\n' == ch || '\r' == ch) {
          if ('\r' == ch) consume_lf();
          if (row.empty()) { any = false; continue; }
          row.emplace_back(); ++row_number_; return check_unchanged();
        } else { field.push_back(static_cast<char>(ch)); state = UNQUOTED; }
      } else if (QUOTED == state) {
        if ('"' == ch) state = AFTER_QUOTE;
        else field.push_back(static_cast<char>(ch));
      } else if (AFTER_QUOTE == state && '"' == ch) {
        field.push_back('"'); state = QUOTED;
      } else if (delimiter_ == ch) {
        row.push_back(field); field.clear(); state = FIELD_START;
      } else if ('\n' == ch || '\r' == ch) {
        if ('\r' == ch) consume_lf();
        row.push_back(field); ++row_number_; return check_unchanged();
      } else if (AFTER_QUOTE == state || '"' == ch) {
        return Status::INVALID;
      } else {
        field.push_back(static_cast<char>(ch));
      }
    }
  }
private:
  void consume_lf() {
    const int next = std::fgetc(file_);
    if ('\n' != next && EOF != next) std::ungetc(next, file_);
  }
  Status check_unchanged() const {
    struct stat current;
    if (0 != fstat(fileno(file_), &current)) return Status::IO_ERROR;
    return (current.st_dev == initial_stat_.st_dev && current.st_ino == initial_stat_.st_ino &&
            current.st_size == initial_stat_.st_size &&
            current.st_mtim.tv_sec == initial_stat_.st_mtim.tv_sec &&
            current.st_mtim.tv_nsec == initial_stat_.st_mtim.tv_nsec)
            ? Status::OK : Status::CHANGED;
  }
  std::FILE *file_;
  char delimiter_;
  size_t row_number_;
  struct stat initial_stat_{};
};


struct FileCsvSchema {
  enum class Kind { UNKNOWN, BOOLEAN, BIGINT, DOUBLE, VARCHAR };
  std::vector<std::string> names;
  std::vector<Kind> kinds;
  size_t rows = 0;
};

inline FileCsvSchema::Kind classify_csv_value(const std::string &value)
{
  if (value.empty() || value == "NULL") return FileCsvSchema::Kind::UNKNOWN;
  if (value == "true" || value == "false") return FileCsvSchema::Kind::BOOLEAN;
  char *end = nullptr;
  errno = 0;
  std::strtoll(value.c_str(), &end, 10);
  if (errno == 0 && end == value.c_str() + value.size()) return FileCsvSchema::Kind::BIGINT;
  errno = 0;
  const double number = std::strtod(value.c_str(), &end);
  if (errno == 0 && end == value.c_str() + value.size() && std::isfinite(number))
    return FileCsvSchema::Kind::DOUBLE;
  return FileCsvSchema::Kind::VARCHAR;
}

inline FileCsvScan::Status infer_csv_schema(FileCsvScan &scan, FileCsvSchema &schema)
{
  schema = FileCsvSchema{};
  FileCsvScan::Status status = FileCsvScan::Status::OK;
  std::vector<std::string> row;
  status = scan.next(row);
  if (status != FileCsvScan::Status::OK || row.empty()) return FileCsvScan::Status::INVALID;
  if (row[0].size() >= 3 && row[0].compare(0, 3, "\xef\xbb\xbf") == 0) row[0].erase(0, 3);
  std::map<std::string, size_t> seen;
  for (std::string &name : row) {
    if (name.empty() || schema.names.size() >= 1024) return FileCsvScan::Status::INVALID;
    const size_t index = ++seen[name];
    if (index > 1) name += "__" + std::to_string(index);
    schema.names.push_back(name);
    schema.kinds.push_back(FileCsvSchema::Kind::UNKNOWN);
  }
  while ((status = scan.next(row)) == FileCsvScan::Status::OK) {
    if (row.size() != schema.names.size()) return FileCsvScan::Status::INVALID;
    ++schema.rows;
    for (size_t i = 0; i < row.size(); ++i) {
      const auto incoming = classify_csv_value(row[i]);
      auto &current = schema.kinds[i];
      if (incoming == FileCsvSchema::Kind::UNKNOWN) continue;
      if (current == FileCsvSchema::Kind::UNKNOWN) current = incoming;
      else if (current == incoming) continue;
      else if ((current == FileCsvSchema::Kind::BIGINT && incoming == FileCsvSchema::Kind::DOUBLE) ||
               (current == FileCsvSchema::Kind::DOUBLE && incoming == FileCsvSchema::Kind::BIGINT))
        current = FileCsvSchema::Kind::DOUBLE;
      else current = FileCsvSchema::Kind::VARCHAR;
    }
  }
  if (status != FileCsvScan::Status::END) return status;
  for (auto &kind : schema.kinds)
    if (kind == FileCsvSchema::Kind::UNKNOWN) kind = FileCsvSchema::Kind::VARCHAR;
  return FileCsvScan::Status::OK;
}

inline FileCsvScan::Status infer_csv_schema(const std::string &path, FileCsvSchema &schema)
{
  FileCsvScan scan;
  const auto status = scan.open(path);
  return status == FileCsvScan::Status::OK ? infer_csv_schema(scan, schema) : status;
}

inline FileCsvScan::Status infer_csv_schema_fd(int fd, FileCsvSchema &schema)
{
  FileCsvScan scan;
  const auto status = scan.open_fd(fd);
  return status == FileCsvScan::Status::OK ? infer_csv_schema(scan, schema) : status;
}

} // namespace sql
} // namespace oceanbase
