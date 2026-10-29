/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include "sql/engine/basic/ob_file_csv_scan.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

namespace oceanbase {
namespace sql {

struct FileJsonCell {
  FileCsvSchema::Kind kind = FileCsvSchema::Kind::UNKNOWN;
  std::string text;
  bool is_null = true;
};

struct FileJsonField {
  std::string name;
  FileJsonCell cell;
};

// One bounded physical line is one JSON object. The parser and its allocator
// belong to this scan; neither is shared between operators or sessions.
class FileJsonScan {
public:
  using Status = FileCsvScan::Status;
  static constexpr size_t MAX_LINE_BYTES = FileCsvScan::MAX_FIELD_BYTES;
  FileJsonScan() : file_(nullptr), row_number_(0) {}
  ~FileJsonScan() { close(); }
  FileJsonScan(const FileJsonScan &) = delete;
  FileJsonScan &operator=(const FileJsonScan &) = delete;

  Status open_fd(int fd)
  {
    close();
    if (fd < 0) return Status::IO_ERROR;
    file_ = ::fdopen(fd, "rb");
    if (file_ == nullptr) {
      ::close(fd);
      return Status::IO_ERROR;
    }
    if (::fstat(::fileno(file_), &initial_stat_) != 0 || !S_ISREG(initial_stat_.st_mode)) {
      close();
      return Status::IO_ERROR;
    }
    row_number_ = 0;
    return Status::OK;
  }

  Status rewind()
  {
    if (file_ == nullptr) return Status::IO_ERROR;
    if (check_unchanged() != Status::OK) return Status::CHANGED;
    if (::fseek(file_, 0, SEEK_SET) != 0) return Status::IO_ERROR;
    row_number_ = 0;
    return Status::OK;
  }

  void close()
  {
    if (file_ != nullptr) { ::fclose(file_); file_ = nullptr; }
    document_.SetNull();
    document_.GetAllocator().Clear();
    row_number_ = 0;
  }

  Status next(std::vector<FileJsonField> &fields)
  {
    fields.clear();
    if (file_ == nullptr) return Status::IO_ERROR;
    if (check_unchanged() != Status::OK) return Status::CHANGED;
    std::string line;
    for (;;) {
      line.clear();
      int ch = EOF;
      while ((ch = ::fgetc(file_)) != EOF && ch != '\n') {
        if (line.size() >= MAX_LINE_BYTES) return Status::INVALID;
        line.push_back(static_cast<char>(ch));
      }
      if (ch == EOF && ::ferror(file_)) return Status::IO_ERROR;
      if (!line.empty() && line.back() == '\r') line.pop_back();
      if (row_number_ == 0 && line.size() >= 3 &&
          line.compare(0, 3, "\xef\xbb\xbf") == 0) line.erase(0, 3);
      if (line.find_first_not_of(" \t\r") == std::string::npos) {
        if (ch == EOF) return Status::END;
        continue;
      }
      break;
    }
    document_.SetNull();
    document_.GetAllocator().Clear();
    document_.Parse<rapidjson::kParseValidateEncodingFlag>(line.data(), line.size());
    if (document_.HasParseError() || !document_.IsObject()) return Status::INVALID;
    for (auto it = document_.MemberBegin(); it != document_.MemberEnd(); ++it) {
      if (!it->name.IsString() || it->name.GetStringLength() == 0 ||
          std::memchr(it->name.GetString(), '\0', it->name.GetStringLength()) != nullptr ||
          fields.size() >= 1024) return Status::INVALID;
      FileJsonField field;
      field.name.assign(it->name.GetString(), it->name.GetStringLength());
      if (std::any_of(fields.begin(), fields.end(), [&field](const FileJsonField &existing) {
          return existing.name == field.name;
        })) return Status::INVALID;
      if (!convert(it->value, field.cell)) return Status::INVALID;
      fields.push_back(std::move(field));
    }
    ++row_number_;
    return check_unchanged();
  }

private:
  static bool convert(const rapidjson::Value &value, FileJsonCell &cell)
  {
    if (value.IsNull()) return true;
    cell.is_null = false;
    if (value.IsString()) {
      cell.text.assign(value.GetString(), value.GetStringLength());
      if (cell.text.size() == 10 && csv_date_prefix(cell.text))
        cell.kind = FileCsvSchema::Kind::DATE;
      else if (csv_datetime_value(cell.text))
        cell.kind = FileCsvSchema::Kind::DATETIME;
      else cell.kind = FileCsvSchema::Kind::VARCHAR;
    } else if (value.IsBool()) {
      cell.kind = FileCsvSchema::Kind::BOOLEAN;
      cell.text = value.GetBool() ? "true" : "false";
    } else if (value.IsInt64()) {
      cell.kind = FileCsvSchema::Kind::BIGINT;
      cell.text = std::to_string(value.GetInt64());
    } else if (value.IsUint64() && value.GetUint64() <=
               static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      cell.kind = FileCsvSchema::Kind::BIGINT;
      cell.text = std::to_string(value.GetUint64());
    } else {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      if (!value.Accept(writer)) return false;
      cell.kind = value.IsNumber() ? FileCsvSchema::Kind::DOUBLE : FileCsvSchema::Kind::VARCHAR;
      cell.text.assign(buffer.GetString(), buffer.GetSize());
    }
    return true;
  }

  Status check_unchanged() const
  {
    struct stat current;
    if (::fstat(::fileno(file_), &current) != 0) return Status::IO_ERROR;
    return current.st_dev == initial_stat_.st_dev &&
           current.st_ino == initial_stat_.st_ino &&
           current.st_size == initial_stat_.st_size &&
           current.st_mtim.tv_sec == initial_stat_.st_mtim.tv_sec &&
           current.st_mtim.tv_nsec == initial_stat_.st_mtim.tv_nsec
        ? Status::OK : Status::CHANGED;
  }

  std::FILE *file_;
  struct stat initial_stat_{};
  size_t row_number_;
  rapidjson::Document document_;
};

inline FileJsonScan::Status infer_json_schema(FileJsonScan &scan, FileCsvSchema &schema)
{
  schema = FileCsvSchema{};
  std::vector<FileJsonField> fields;
  FileJsonScan::Status status;
  while ((status = scan.next(fields)) == FileJsonScan::Status::OK) {
    ++schema.rows;
    for (const auto &field : fields) {
      auto it = std::find(schema.names.begin(), schema.names.end(), field.name);
      size_t index;
      if (it == schema.names.end()) {
        if (schema.names.size() >= 1024) return FileJsonScan::Status::INVALID;
        index = schema.names.size();
        schema.names.push_back(field.name);
        schema.kinds.push_back(FileCsvSchema::Kind::UNKNOWN);
        schema.datetime_scales.push_back(0);
        schema.nullable.push_back(schema.rows > 1);
      } else {
        index = static_cast<size_t>(it - schema.names.begin());
      }
      if (field.cell.is_null) {
        schema.nullable[index] = true;
        continue;
      }
      if (field.cell.kind == FileCsvSchema::Kind::DATETIME && field.cell.text.size() > 19) {
        const int8_t scale = static_cast<int8_t>(field.cell.text.size() - 20);
        if (scale > schema.datetime_scales[index]) schema.datetime_scales[index] = scale;
      }
      auto &kind = schema.kinds[index];
      const auto incoming = field.cell.kind;
      if (kind == FileCsvSchema::Kind::UNKNOWN) kind = incoming;
      else if (kind == incoming) continue;
      else if ((kind == FileCsvSchema::Kind::BIGINT && incoming == FileCsvSchema::Kind::DOUBLE) ||
               (kind == FileCsvSchema::Kind::DOUBLE && incoming == FileCsvSchema::Kind::BIGINT))
        kind = FileCsvSchema::Kind::DOUBLE;
      else if ((kind == FileCsvSchema::Kind::DATE && incoming == FileCsvSchema::Kind::DATETIME) ||
               (kind == FileCsvSchema::Kind::DATETIME && incoming == FileCsvSchema::Kind::DATE))
        kind = FileCsvSchema::Kind::DATETIME;
      else kind = FileCsvSchema::Kind::VARCHAR;
    }
    for (size_t i = 0; i < schema.names.size(); ++i) {
      const bool present = std::any_of(fields.begin(), fields.end(), [&schema, i](const FileJsonField &field) {
        return field.name == schema.names[i];
      });
      if (!present) schema.nullable[i] = true;
    }
  }
  if (status != FileJsonScan::Status::END || schema.names.empty()) return FileJsonScan::Status::INVALID;
  for (auto &kind : schema.kinds)
    if (kind == FileCsvSchema::Kind::UNKNOWN) kind = FileCsvSchema::Kind::VARCHAR;
  return FileJsonScan::Status::OK;
}

inline FileJsonScan::Status infer_json_schema_fd(int fd, FileCsvSchema &schema)
{
  FileJsonScan scan;
  const auto status = scan.open_fd(fd);
  return status == FileJsonScan::Status::OK ? infer_json_schema(scan, schema) : status;
}

} // namespace sql
} // namespace oceanbase
