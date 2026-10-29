/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include "sql/engine/basic/ob_file_json_scan.h"
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace oceanbase {
namespace sql {

// The file SQL reader intentionally supports flat, scalar Parquet columns.
// Unsupported encodings, logical types and codecs fail closed; they must never
// be interpreted as plain values. Only metadata and one page per column live
// in memory, regardless of the number of rows or row groups.
class FileParquetScan {
public:
  using Status = FileCsvScan::Status;
  FileParquetScan() : fd_(-1), metadata_offset_(0), group_index_(0), row_index_(0) {}
  ~FileParquetScan() { close(); }
  FileParquetScan(const FileParquetScan &) = delete;
  FileParquetScan &operator=(const FileParquetScan &) = delete;

  Status open_fd(int fd)
  {
    close();
    if (fd < 0) return Status::IO_ERROR;
    fd_ = fd;
    if (::fstat(fd_, &initial_stat_) != 0 || !S_ISREG(initial_stat_.st_mode) ||
        initial_stat_.st_size < 12 || !load_metadata()) {
      close();
      return Status::INVALID;
    }
    return rewind();
  }

  const FileCsvSchema &schema() const { return schema_; }

  Status rewind()
  {
    if (fd_ < 0 || !unchanged()) return Status::CHANGED;
    group_index_ = row_index_ = 0;
    columns_.clear();
    return Status::OK;
  }

  void close()
  {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    groups_.clear(); columns_.clear(); kinds_.clear(); schema_ = FileCsvSchema{};
    metadata_offset_ = group_index_ = row_index_ = 0;
  }

  Status next(std::vector<FileJsonCell> &row)
  {
    row.clear();
    if (fd_ < 0) return Status::IO_ERROR;
    if (!unchanged()) return Status::CHANGED;
    while (group_index_ < groups_.size() && row_index_ == groups_[group_index_].rows) {
      ++group_index_; row_index_ = 0; columns_.clear();
    }
    if (group_index_ == groups_.size()) return Status::END;
    if (columns_.empty() && !open_group()) return Status::INVALID;
    for (size_t i = 0; i < columns_.size(); ++i) {
      ColumnCursor &cursor = columns_[i];
      if (cursor.page_index == cursor.page.size() && !load_page(i)) return Status::INVALID;
      if (cursor.page_index >= cursor.page.size()) return Status::INVALID;
      row.push_back(cursor.page[cursor.page_index++]);
    }
    ++row_index_;
    return unchanged() ? Status::OK : Status::CHANGED;
  }

private:
  struct Value {
    int64_t integer = 0;
    std::string string;
    std::vector<Value> list;
    std::map<int, Value> fields;
    const Value &at(int id) const
    {
      auto it = fields.find(id);
      return it == fields.end() ? missing() : it->second;
    }
    bool has(int id) const { return fields.find(id) != fields.end(); }
    static const Value &missing() { static const Value empty; return empty; }
  };

  struct Compact {
    const uint8_t *p;
    const uint8_t *end;
    bool ok = true;
    bool var(uint64_t &out)
    {
      out = 0;
      for (int i = 0; i < 10; ++i) {
        if (p == end) return ok = false;
        const uint8_t c = *p++;
        if (i == 9 && c > 1) return ok = false;
        out |= uint64_t(c & 127) << (7 * i);
        if (!(c & 128)) return true;
      }
      return ok = false;
    }
    bool decode(uint8_t type, Value &v, int depth)
    {
      if (depth > 24 || !ok) return ok = false;
      uint64_t number;
      if (type == 1 || type == 2) { v.integer = type == 1; return true; }
      if (type == 3) {
        if (p == end) return ok = false;
        v.integer = *p++; return true;
      }
      if (type >= 4 && type <= 6) {
        if (!var(number)) return false;
        v.integer = static_cast<int64_t>((number >> 1) ^ (0 - (number & 1))); return true;
      }
      if (type == 7) {
        if (end - p < 8) return ok = false;
        p += 8; return true;
      }
      if (type == 8) {
        if (!var(number) || number > static_cast<uint64_t>(end - p)) return ok = false;
        v.string.assign(reinterpret_cast<const char *>(p), static_cast<size_t>(number));
        p += number; return true;
      }
      if (type == 9 || type == 10) {
        if (p == end) return ok = false;
        const uint8_t header = *p++;
        uint64_t count = header >> 4;
        if (count == 15 && !var(count)) return false;
        if (count > 100000 || count > static_cast<uint64_t>(end - p) + 1) return ok = false;
        v.list.resize(static_cast<size_t>(count));
        for (Value &item : v.list) if (!decode(header & 15, item, depth + 1)) return false;
        return true;
      }
      if (type == 11) { // map
        if (!var(number) || number > 100000 || p == end) return ok = false;
        const uint8_t types = *p++;
        for (uint64_t i = 0; i < number; ++i) {
          Value key, val;
          if (!decode(types >> 4, key, depth + 1) ||
              !decode(types & 15, val, depth + 1)) return false;
        }
        return true;
      }
      if (type == 12) return structure(v, depth + 1);
      return ok = false;
    }
    bool structure(Value &v, int depth = 0)
    {
      int last = 0;
      while (ok && p < end) {
        const uint8_t header = *p++;
        if (header == 0) return true;
        uint64_t raw = 0;
        if ((header >> 4) == 0 && !var(raw)) return false;
        const int id = (header >> 4) ? last + (header >> 4) :
            static_cast<int>((raw >> 1) ^ (0 - (raw & 1)));
        if (id <= 0 || id > 32767 || v.has(id)) return ok = false;
        last = id;
        if (!decode(header & 15, v.fields[id], depth + 1)) return false;
      }
      return ok = false;
    }
  };

  struct ColumnInfo {
    int physical = -1;
    int logical = -1;
    int repetition = -1;
    int scale = 0;
  };
  struct Chunk { int64_t start = 0, end = 0, values = 0; int codec = 0; };
  struct Group { int64_t rows = 0; std::vector<Chunk> chunks; };
  struct ColumnCursor {
    int64_t offset = 0, end = 0;
    int codec = 0;
    std::vector<FileJsonCell> dictionary;
    std::vector<FileJsonCell> page;
    size_t page_index = 0;
  };

  static uint32_t little32(const uint8_t *p)
  { return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24; }
  static uint64_t little64(const uint8_t *p)
  { return uint64_t(little32(p)) | uint64_t(little32(p + 4)) << 32; }
  bool read_at(int64_t offset, void *out, size_t length) const
  {
    if (offset < 0 || offset > initial_stat_.st_size ||
        length > static_cast<uint64_t>(initial_stat_.st_size - offset)) return false;
    uint8_t *p = static_cast<uint8_t *>(out);
    while (length) {
      const ssize_t n = ::pread(fd_, p, length, offset);
      if (n <= 0) return false;
      p += n; offset += n; length -= n;
    }
    return true;
  }
  bool unchanged() const
  {
    struct stat now;
    return fd_ >= 0 && ::fstat(fd_, &now) == 0 &&
        now.st_dev == initial_stat_.st_dev && now.st_ino == initial_stat_.st_ino &&
        now.st_size == initial_stat_.st_size &&
        now.st_mtim.tv_sec == initial_stat_.st_mtim.tv_sec &&
        now.st_mtim.tv_nsec == initial_stat_.st_mtim.tv_nsec;
  }
  static bool valid_index(int64_t i, size_t n) { return i >= 0 && static_cast<uint64_t>(i) < n; }
  bool load_metadata()
  {
    uint8_t marker[4], footer[8];
    if (!read_at(0, marker, 4) || std::memcmp(marker, "PAR1", 4) ||
        !read_at(initial_stat_.st_size - 8, footer, 8) ||
        std::memcmp(footer + 4, "PAR1", 4)) return false;
    const uint32_t length = little32(footer);
    if (length == 0 || length > 16 * 1024 * 1024 ||
        length > static_cast<uint64_t>(initial_stat_.st_size - 12)) return false;
    metadata_offset_ = initial_stat_.st_size - 8 - length;
    std::vector<uint8_t> data(length);
    if (!read_at(metadata_offset_, data.data(), length)) return false;
    Compact protocol{data.data(), data.data() + data.size()};
    Value root;
    if (!protocol.structure(root) || !root.has(2) || !root.has(4) || root.at(2).list.empty() ||
        root.at(2).list.size() > 1025 || root.at(3).integer < 0) return false;
    const auto &nodes = root.at(2).list;
    if (nodes[0].at(5).integer != static_cast<int64_t>(nodes.size() - 1)) return false;
    for (size_t i = 1; i < nodes.size(); ++i) {
      const auto &node = nodes[i];
      const int physical = node.at(1).integer;
      int logical = node.has(6) ? node.at(6).integer : -1;
      if (node.has(10)) {
        const Value &modern = node.at(10);
        int modern_type = -1;
        if (modern.has(1)) modern_type = 0; // STRING
        else if (modern.has(6)) modern_type = 6; // DATE
        else if (modern.has(8)) { // TIMESTAMP
          const Value &unit = modern.at(8).at(2);
          modern_type = unit.has(1) ? 9 : unit.has(2) ? 10 : -1;
        }
        if (modern_type < 0 || (logical >= 0 && logical != modern_type)) return false;
        logical = modern_type;
      }
      const int repetition = node.at(3).integer;
      if (!node.has(1) || !node.has(3) || !node.has(4) ||
          node.at(4).string.empty() || repetition < 0 || repetition > 1 ||
          std::find(schema_.names.begin(), schema_.names.end(), node.at(4).string) !=
              schema_.names.end() ||
          !((physical == 0 && logical == -1) ||
            ((physical == 1 || physical == 2) && (logical == -1 || logical == 6 ||
              logical == 9 || logical == 10)) ||
            ((physical == 4 || physical == 5) && logical == -1) ||
            (physical == 6 && logical == 0))) return false;
      ColumnInfo info{physical, logical, repetition, 0};
      kinds_.push_back(info);
      schema_.names.push_back(node.at(4).string);
      auto kind = FileCsvSchema::Kind::UNKNOWN;
      if (physical == 0) kind = FileCsvSchema::Kind::BOOLEAN;
      else if (logical == 6) kind = FileCsvSchema::Kind::DATE;
      else if (logical == 9 || logical == 10) kind = FileCsvSchema::Kind::DATETIME;
      else if (physical == 1 || physical == 2) kind = FileCsvSchema::Kind::BIGINT;
      else if (physical == 4 || physical == 5) kind = FileCsvSchema::Kind::DOUBLE;
      else kind = FileCsvSchema::Kind::VARCHAR;
      schema_.kinds.push_back(kind);
      schema_.nullable.push_back(repetition == 1);
      schema_.datetime_scales.push_back(logical == 9 ? 3 : logical == 10 ? 6 : 0);
    }
    if (schema_.names.empty() || root.at(4).list.size() > 100000) return false;
    int64_t total_rows = 0;
    for (const auto &entry : root.at(4).list) {
      Group group;
      group.rows = entry.at(3).integer;
      if (group.rows < 0 || entry.at(1).list.size() != kinds_.size()) return false;
      for (size_t i = 0; i < kinds_.size(); ++i) {
        const Value &chunk = entry.at(1).list[i];
        const Value &meta = chunk.at(3);
        if (chunk.has(1) && !chunk.at(1).string.empty()) return false;
        if (!entry.at(1).list[i].has(3) || meta.at(1).integer != kinds_[i].physical ||
            meta.at(4).integer < 0 || meta.at(4).integer > 1 ||
            meta.at(3).list.size() != 1 ||
            meta.at(3).list[0].string != schema_.names[i] ||
            meta.at(5).integer != group.rows || !meta.has(9) || !meta.has(7)) return false;
        const int64_t start = meta.has(11) ? meta.at(11).integer : meta.at(9).integer;
        const int64_t size = meta.at(7).integer;
        if (start < 4 || size < 0 || start > metadata_offset_ || size > metadata_offset_ - start)
          return false;
        group.chunks.push_back({start, start + size, group.rows,
                                static_cast<int>(meta.at(4).integer)});
      }
      groups_.push_back(std::move(group)); total_rows += group.rows;
      if (total_rows < 0) return false;
    }
    schema_.rows = total_rows;
    return total_rows == root.at(3).integer && unchanged();
  }

  static bool snappy(const uint8_t *input, size_t length, size_t expected,
                     std::vector<uint8_t> &out)
  {
    const uint8_t *p = input, *end = input + length;
    Compact reader{p, end}; uint64_t size;
    if (!reader.var(size) || size != expected) return false;
    p = reader.p; out.clear(); out.reserve(expected);
    while (p < end && out.size() < expected) {
      const uint8_t tag = *p++;
      size_t count = 0, offset = 0;
      if ((tag & 3) == 0) {
        count = (tag >> 2) + 1;
        if (count > 60) {
          const size_t bytes = count - 60;
          if (bytes > 4 || static_cast<size_t>(end - p) < bytes) return false;
          count = 1;
          for (size_t i = 0; i < bytes; ++i) count += size_t(*p++) << (8 * i);
        }
        if (count > expected - out.size() || static_cast<size_t>(end - p) < count) return false;
        out.insert(out.end(), p, p + count); p += count;
      } else {
        const int kind = tag & 3;
        const int bytes = kind == 1 ? 1 : kind == 2 ? 2 : 4;
        if (end - p < bytes) return false;
        count = kind == 1 ? 4 + ((tag >> 2) & 7) : 1 + (tag >> 2);
        offset = kind == 1 ? (size_t(tag & 0xe0) << 3) : 0;
        for (int i = 0; i < bytes; ++i) offset |= size_t(*p++) << (8 * i);
        if (!offset || offset > out.size() || count > expected - out.size()) return false;
        for (size_t i = 0; i < count; ++i) out.push_back(out[out.size() - offset]);
      }
    }
    return p == end && out.size() == expected;
  }

  static bool levels(const uint8_t *data, size_t length, int width, size_t count,
                     std::vector<uint32_t> &result)
  {
    result.clear(); result.reserve(count);
    const uint8_t *p = data, *end = data + length;
    while (result.size() < count) {
      Compact reader{p, end}; uint64_t header;
      if (!reader.var(header) || header == 0) return false;
      p = reader.p;
      if ((header & 1) == 0) {
        const size_t bytes = (width + 7) / 8;
        if (end - p < static_cast<ptrdiff_t>(bytes) || (header >> 1) > count - result.size())
          return false;
        uint32_t value = 0;
        for (size_t i = 0; i < bytes; ++i) value |= uint32_t(*p++) << (8 * i);
        result.insert(result.end(), static_cast<size_t>(header >> 1), value);
      } else {
        const size_t groups = header >> 1;
        if (groups > (count - result.size() + 7) / 8 ||
            (width != 0 && groups > static_cast<size_t>(end - p) / width)) return false;
        for (size_t i = 0; i < groups; ++i) {
          uint64_t bits = 0;
          for (int j = 0; j < width; ++j) bits |= uint64_t(*p++) << (8 * j);
          for (int j = 0; j < 8 && result.size() < count; ++j)
            result.push_back((bits >> (j * width)) & ((1U << width) - 1));
        }
      }
    }
    return true;
  }

  static bool value_at(const uint8_t *&p, const uint8_t *end, const ColumnInfo &info,
                       FileJsonCell &cell)
  {
    const int physical = info.physical;
    const size_t bytes = physical == 0 ? 0 : physical == 1 || physical == 4 ? 4 :
                         physical == 2 || physical == 5 ? 8 : 4;
    if (static_cast<size_t>(end - p) < bytes) return false;
    cell.is_null = false;
    if (physical == 0) return false; // PLAIN booleans are bit packed; decoded separately.
    if (physical == 6) {
      const uint32_t size = little32(p); p += 4;
      if (size > static_cast<size_t>(end - p)) return false;
      cell.text.assign(reinterpret_cast<const char *>(p), size); p += size;
      cell.kind = FileCsvSchema::Kind::VARCHAR;
      return true;
    }
    const uint64_t raw = bytes == 4 ? little32(p) : little64(p);
    p += bytes;
    if (info.logical == 6 || info.logical == 9 || info.logical == 10) {
      int64_t micros = info.logical == 6 ? static_cast<int32_t>(raw) * int64_t(86400000000) :
          info.logical == 9 ? static_cast<int64_t>(raw) * 1000 : static_cast<int64_t>(raw);
      time_t seconds = static_cast<time_t>(micros / 1000000);
      int fraction = static_cast<int>(micros % 1000000);
      if (fraction < 0) { --seconds; fraction += 1000000; }
      struct tm tm_value;
      if (!gmtime_r(&seconds, &tm_value)) return false;
      char buffer[96];
      if (info.logical == 6) {
        std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", tm_value.tm_year + 1900,
                      tm_value.tm_mon + 1, tm_value.tm_mday);
        cell.kind = FileCsvSchema::Kind::DATE;
      } else {
        std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                      tm_value.tm_year + 1900, tm_value.tm_mon + 1, tm_value.tm_mday,
                      tm_value.tm_hour, tm_value.tm_min, tm_value.tm_sec, fraction);
        cell.kind = FileCsvSchema::Kind::DATETIME;
      }
      cell.text = buffer;
    } else if (physical == 4 || physical == 5) {
      double value;
      if (physical == 4) { const uint32_t bits = raw; float f; std::memcpy(&f, &bits, 4); value = f; }
      else std::memcpy(&value, &raw, 8);
      if (!std::isfinite(value)) return false;
      char buffer[64]; std::snprintf(buffer, sizeof(buffer), "%.17g", value);
      cell.text = buffer; cell.kind = FileCsvSchema::Kind::DOUBLE;
    } else {
      cell.text = std::to_string(physical == 1 ? static_cast<int64_t>(static_cast<int32_t>(raw)) :
                                 static_cast<int64_t>(raw));
      cell.kind = FileCsvSchema::Kind::BIGINT;
    }
    return true;
  }

  bool open_group()
  {
    const Group &group = groups_[group_index_];
    columns_.resize(group.chunks.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      columns_[i].offset = group.chunks[i].start;
      columns_[i].end = group.chunks[i].end;
      columns_[i].codec = group.chunks[i].codec;
    }
    return true;
  }

  bool load_page(size_t index)
  {
    ColumnCursor &cursor = columns_[index];
    const ColumnInfo &info = kinds_[index];
    while (cursor.offset < cursor.end) {
      const size_t header_bytes = static_cast<size_t>(std::min<int64_t>(4096, cursor.end - cursor.offset));
      std::vector<uint8_t> header(header_bytes);
      if (!read_at(cursor.offset, header.data(), header_bytes)) return false;
      Compact protocol{header.data(), header.data() + header.size()}; Value page;
      if (!protocol.structure(page)) return false;
      const int64_t used = protocol.p - header.data();
      const int64_t compressed = page.at(3).integer, uncompressed = page.at(2).integer;
      if (!page.has(1) || !page.has(2) || !page.has(3) ||
          compressed < 0 || compressed > 16 * 1024 * 1024 ||
          uncompressed < 0 || uncompressed > 16 * 1024 * 1024 ||
          used > cursor.end - cursor.offset || compressed > cursor.end - cursor.offset - used)
        return false;
      cursor.offset += used;
      std::vector<uint8_t> encoded(static_cast<size_t>(compressed));
      if (!read_at(cursor.offset, encoded.data(), encoded.size())) return false;
      cursor.offset += compressed;
      std::vector<uint8_t> decoded;
      const int page_type = page.at(1).integer;
      if (page_type == 2 || page_type == 0) { // dictionary or V1 data page
        if (cursor.codec == 0) decoded = std::move(encoded);
        else if (cursor.codec != 1 || !snappy(encoded.data(), encoded.size(), uncompressed, decoded))
          return false;
        if (decoded.size() != static_cast<size_t>(uncompressed)) return false;
      } else if (page_type == 3) { // V2: level bytes are never compressed.
        const Value &v2 = page.at(8);
        const int64_t level_size = v2.at(5).integer + v2.at(6).integer;
        if (!page.has(8) || level_size < 0 || level_size > compressed ||
            level_size > uncompressed) return false;
        decoded.assign(encoded.begin(), encoded.begin() + level_size);
        if (cursor.codec == 0 || (v2.has(7) && !v2.at(7).integer)) {
          decoded.insert(decoded.end(), encoded.begin() + level_size, encoded.end());
        } else if (cursor.codec == 1) {
          std::vector<uint8_t> values;
          if (!snappy(encoded.data() + level_size, encoded.size() - level_size,
                      uncompressed - level_size, values)) return false;
          decoded.insert(decoded.end(), values.begin(), values.end());
        } else return false;
        if (decoded.size() != static_cast<size_t>(uncompressed)) return false;
      } else return false;
      const uint8_t *p = decoded.data(), *end = p + decoded.size();
      if (page_type == 2) {
        const Value &dictionary = page.at(7);
        const int count = dictionary.at(1).integer;
        if (!page.has(7) || dictionary.at(2).integer != 0 || count < 0 || count > 1000000)
          return false;
        cursor.dictionary.clear();
        if (info.physical == 0) return false;
        for (int i = 0; i < count; ++i) {
          FileJsonCell cell;
          if (!value_at(p, end, info, cell)) return false;
          cursor.dictionary.push_back(std::move(cell));
        }
        if (p != end) return false;
        continue;
      }
      const Value &data_header = page_type == 3 ? page.at(8) : page.at(5);
      if ((page_type == 3 && !page.has(8)) || (page_type == 0 && !page.has(5))) return false;
      const int64_t count = data_header.at(1).integer;
      const int encoding = data_header.at(page_type == 3 ? 4 : 2).integer;
      if (count <= 0 || count > 1000000 || (encoding != 0 && encoding != 2 && encoding != 8))
        return false;
      if (page_type == 3 && data_header.at(3).integer != count) return false;
      std::vector<uint32_t> defs;
      if (page_type == 3) {
        if (data_header.at(6).integer != 0) return false; // no repeated columns
        const size_t length = static_cast<size_t>(data_header.at(5).integer);
        if (length > decoded.size() ||
            (info.repetition == 1 && !levels(p, length, 1, count, defs))) return false;
        p += length;
        if (info.repetition == 1 &&
            static_cast<int64_t>(std::count(defs.begin(), defs.end(), 0)) !=
                data_header.at(2).integer) return false;
      } else if (info.repetition == 1) {
        if (data_header.at(3).integer != 3 || end - p < 4) return false; // RLE levels
        const size_t length = little32(p); p += 4;
        if (length > static_cast<size_t>(end - p) || !levels(p, length, 1, count, defs))
          return false;
        p += length;
      }
      cursor.page.clear(); cursor.page.reserve(count); cursor.page_index = 0;
      std::vector<uint32_t> indices;
      if (encoding != 0) {
        if (p == end) return false;
        const int width = *p++;
        const size_t non_null_count = info.repetition == 0 ? count :
            static_cast<size_t>(std::count(defs.begin(), defs.end(), 1));
        if (width < 0 || width > 16 ||
            !levels(p, end - p, width, non_null_count, indices)) return false;
      }
      size_t non_null = 0;
      for (int64_t i = 0; i < count; ++i) {
        FileJsonCell cell;
        if (info.repetition == 0 || defs[i] == 1) {
          if (encoding == 0) {
            if (info.physical == 0) {
              const size_t byte = non_null / 8;
              if (byte >= static_cast<size_t>(end - p)) return false;
              cell.is_null = false; cell.kind = FileCsvSchema::Kind::BOOLEAN;
              cell.text = (p[byte] & (1 << (non_null % 8))) ? "true" : "false";
            } else if (!value_at(p, end, info, cell)) return false;
          } else {
            if (!valid_index(indices[non_null], cursor.dictionary.size())) return false;
            cell = cursor.dictionary[indices[non_null]];
          }
          ++non_null;
        } else if (defs[i] != 0) return false;
        cursor.page.push_back(std::move(cell));
      }
      if (encoding == 0 && info.physical == 0) p += (non_null + 7) / 8;
      if (encoding == 0 && p != end) return false;
      return true;
    }
    return false;
  }

  int fd_;
  struct stat initial_stat_{};
  int64_t metadata_offset_;
  size_t group_index_;
  int64_t row_index_;
  FileCsvSchema schema_;
  std::vector<ColumnInfo> kinds_;
  std::vector<Group> groups_;
  std::vector<ColumnCursor> columns_;
};

} // namespace sql
} // namespace oceanbase
