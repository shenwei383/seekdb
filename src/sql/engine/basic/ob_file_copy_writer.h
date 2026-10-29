/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_COPY_WRITER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_COPY_WRITER_H_

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/openat2.h>
#include <sys/syscall.h>
#endif
#include "lib/ob_errno.h"

namespace oceanbase { namespace sql {

// COPY writes a sibling temporary file and publishes it with RENAME_NOREPLACE.
// The directory descriptor is pinned beneath secure_file_priv with openat2.
class FileCopyWriter {
public:
  FileCopyWriter() : fd_(-1), dir_fd_(-1) {}
  ~FileCopyWriter() { close(); }
  FileCopyWriter(const FileCopyWriter &) = delete;
  FileCopyWriter &operator=(const FileCopyWriter &) = delete;

  int open(const std::string &file_path, const std::string &secure_root)
  {
#ifdef __linux__
    close();
    if (file_path.empty() || file_path[0] != '/' || secure_root.empty() ||
        file_path.find('\0') != std::string::npos) return common::OB_INVALID_ARGUMENT;
    const size_t slash = file_path.rfind('/');
    if (slash == std::string::npos || slash + 1 >= file_path.size()) return common::OB_INVALID_ARGUMENT;
    basename_ = file_path.substr(slash + 1);
    if (basename_ == "." || basename_ == "..") return common::OB_INVALID_ARGUMENT;
    const std::string parent_path = slash == 0 ? "/" : file_path.substr(0, slash);
    char parent_buf[PATH_MAX + 1];
    char root_buf[PATH_MAX + 1];
    if (realpath(parent_path.c_str(), parent_buf) == nullptr ||
        realpath(secure_root.c_str(), root_buf) == nullptr) return common::OB_FILE_NOT_EXIST;
    const std::string parent(parent_buf), root(root_buf);
    if (root != "/" && parent != root &&
        (parent.size() <= root.size() || parent.compare(0, root.size(), root) != 0 ||
         parent[root.size()] != '/')) return common::OB_ERR_NO_PRIVILEGE;

    int slash_fd = ::open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
    if (slash_fd < 0) return common::OB_IO_ERROR;
    int root_fd = slash_fd;
    if (root != "/") {
      struct open_how how = {};
      how.flags = O_PATH | O_DIRECTORY | O_CLOEXEC;
      how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;
      root_fd = static_cast<int>(::syscall(SYS_openat2, slash_fd, root.c_str() + 1,
                                          &how, sizeof(how)));
      const int saved_errno = errno;
      ::close(slash_fd);
      errno = saved_errno;
      if (root_fd < 0) return common::OB_ERR_NO_PRIVILEGE;
    }
    struct open_how dir_how = {};
    dir_how.flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
    dir_how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;
    if (parent == root) {
      dir_fd_ = static_cast<int>(::syscall(SYS_openat2, root_fd, ".", &dir_how, sizeof(dir_how)));
    } else {
      const char *relative = parent.c_str() + (root == "/" ? 1 : root.size() + 1);
      dir_fd_ = static_cast<int>(::syscall(SYS_openat2, root_fd, relative,
                                           &dir_how, sizeof(dir_how)));
    }
    ::close(root_fd);
    if (dir_fd_ < 0) return common::OB_ERR_NO_PRIVILEGE;
    struct stat st;
    if (::fstatat(dir_fd_, basename_.c_str(), &st, AT_SYMLINK_NOFOLLOW) == 0) {
      return common::OB_FILE_ALREADY_EXIST;
    } else if (errno != ENOENT) {
      return common::OB_IO_ERROR;
    }
    struct open_how file_how = {};
    file_how.flags = O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC;
    file_how.mode = 0600;
    file_how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;
    for (int attempt = 0; attempt < 16 && fd_ < 0; ++attempt) {
      struct timespec now;
      ::clock_gettime(CLOCK_REALTIME, &now);
      const std::string name = ".seekdb-copy-" + std::to_string(::getpid()) + "-" +
                               std::to_string(now.tv_sec) + "-" +
                               std::to_string(now.tv_nsec) + "-" + std::to_string(attempt);
      fd_ = static_cast<int>(::syscall(SYS_openat2, dir_fd_, name.c_str(),
                                       &file_how, sizeof(file_how)));
      if (fd_ >= 0) temp_name_ = name;
      if (fd_ < 0 && errno != EEXIST) return common::OB_IO_ERROR;
    }
    return fd_ < 0 ? common::OB_FILE_ALREADY_EXIST : common::OB_SUCCESS;
#else
    (void)file_path;
    (void)secure_root;
    return common::OB_NOT_SUPPORTED;
#endif
  }

  int append(const char *data, size_t size)
  {
    if (fd_ < 0) return common::OB_NOT_INIT;
    while (size > 0) {
      const ssize_t wrote = ::write(fd_, data, size);
      if (wrote < 0 && errno == EINTR) continue;
      if (wrote <= 0) return common::OB_IO_ERROR;
      data += wrote;
      size -= static_cast<size_t>(wrote);
    }
    return common::OB_SUCCESS;
  }

  int finish()
  {
#ifdef __linux__
    if (fd_ < 0 || dir_fd_ < 0) return common::OB_NOT_INIT;
    if (::fsync(fd_) != 0) return common::OB_IO_ERROR;
    ::close(fd_);
    fd_ = -1;
#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1 << 0)
#endif
    if (::syscall(SYS_renameat2, dir_fd_, temp_name_.c_str(), dir_fd_,
                  basename_.c_str(), RENAME_NOREPLACE) != 0) {
      return errno == EEXIST ? common::OB_FILE_ALREADY_EXIST : common::OB_IO_ERROR;
    }
    temp_name_.clear();
    (void)::fsync(dir_fd_);
    return common::OB_SUCCESS;
#else
    return common::OB_NOT_SUPPORTED;
#endif
  }

  void close()
  {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    if (dir_fd_ >= 0) {
      if (!temp_name_.empty()) (void)::unlinkat(dir_fd_, temp_name_.c_str(), 0);
      ::close(dir_fd_);
      dir_fd_ = -1;
    }
    std::string().swap(temp_name_);
    std::string().swap(basename_);
  }

private:
  int fd_;
  int dir_fd_;
  std::string basename_;
  std::string temp_name_;
};

} } // namespace oceanbase::sql
#endif
