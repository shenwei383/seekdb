/* Copyright (c) 2025 OceanBase. Licensed under the Apache License, Version 2.0. */
#include "sql/engine/basic/ob_file_json_scan.h"
#include <cassert>
#include <fstream>
#include <iostream>
#include <sys/resource.h>

using oceanbase::sql::FileCsvSchema;
using oceanbase::sql::FileJsonField;
using oceanbase::sql::FileJsonScan;
using oceanbase::sql::infer_json_schema_fd;
using oceanbase::sql::open_secure_csv_fd;

int main()
{
  char temp[] = "./build_release/file_json_scan_XXXXXX";
  const char *directory = mkdtemp(temp);
  assert(directory != nullptr);
  char root_buf[PATH_MAX + 1];
  assert(realpath(directory, root_buf) != nullptr);
  const std::string root(root_buf);
  const std::string path = root + "/rows.jsonl";
  {
    std::ofstream out(path);
    out << "{\"id\":1,\"name\":\"Alice\",\"flag\":true,\"note\":\"NULL\"}\r\n";
    out << "{\"id\":2,\"flag\":false,\"note\":null,\"extra\":[1,2]}\n";
  }
  FileCsvSchema schema;
  assert(infer_json_schema_fd(open_secure_csv_fd(root, path), schema) == FileJsonScan::Status::OK);
  assert(schema.rows == 2 && schema.names.size() == 5);
  assert(schema.names[0] == "id" && schema.kinds[0] == FileCsvSchema::Kind::BIGINT);
  assert(schema.names[1] == "name" && schema.nullable[1]);
  assert(schema.names[2] == "flag" && schema.kinds[2] == FileCsvSchema::Kind::BOOLEAN);
  assert(schema.names[3] == "note" && schema.nullable[3]);
  assert(schema.names[4] == "extra" && schema.nullable[4]);
  {
    FileJsonScan scan;
    assert(scan.open_fd(open_secure_csv_fd(root, path)) == FileJsonScan::Status::OK);
    std::vector<FileJsonField> row;
    assert(scan.next(row) == FileJsonScan::Status::OK && row[0].cell.text == "1");
    assert(row[3].cell.text == "NULL" && !row[3].cell.is_null);
    assert(scan.next(row) == FileJsonScan::Status::OK && row[2].name == "note" && row[2].cell.is_null);
    assert(row[3].name == "extra" && row[3].cell.text == "[1,2]");
    assert(scan.next(row) == FileJsonScan::Status::END);
    assert(scan.rewind() == FileJsonScan::Status::OK);
    assert(scan.next(row) == FileJsonScan::Status::OK && row[0].cell.text == "1");
    std::ofstream edit(path, std::ios::app);
    edit << "{}\n";
    edit.close();
    assert(scan.next(row) == FileJsonScan::Status::CHANGED);
  }
  const std::string bad = root + "/bad.jsonl";
  { std::ofstream out(bad); out << "{\"id\":1}\n{\"id\":\n"; }
  assert(infer_json_schema_fd(open_secure_csv_fd(root, bad), schema) == FileJsonScan::Status::INVALID);
  const std::string duplicate = root + "/duplicate.jsonl";
  { std::ofstream out(duplicate); out << "{\"id\":1,\"id\":2}\n"; }
  assert(infer_json_schema_fd(open_secure_csv_fd(root, duplicate), schema) == FileJsonScan::Status::INVALID);
  const std::string large = root + "/large.jsonl";
  {
    std::ofstream out(large);
    for (int i = 0; i < 1000000; ++i) out << "{\"id\":" << i << ",\"value\":" << i % 1000 << "}\n";
  }
  assert(infer_json_schema_fd(open_secure_csv_fd(root, large), schema) == FileJsonScan::Status::OK);
  assert(schema.rows == 1000000 && schema.names.size() == 2);
  struct rusage usage = {};
  assert(getrusage(RUSAGE_SELF, &usage) == 0);
  assert(usage.ru_maxrss < 32768);
  std::cout << "jsonl scan PASS; 1000000 rows; peak RSS " << usage.ru_maxrss << " KiB\n";
  assert(unlink(path.c_str()) == 0);
  assert(unlink(bad.c_str()) == 0);
  assert(unlink(duplicate.c_str()) == 0);
  assert(unlink(large.c_str()) == 0);
  assert(rmdir(root.c_str()) == 0);
}
