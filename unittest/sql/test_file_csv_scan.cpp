/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#include "sql/engine/basic/ob_file_csv_scan.h"
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/resource.h>

using oceanbase::sql::FileCsvScan;
using oceanbase::sql::FileCsvSchema;
using oceanbase::sql::infer_csv_schema_fd;
using oceanbase::sql::open_secure_csv_fd;
using oceanbase::sql::classify_csv_value;

static long peak_rss_kb()
{
  struct rusage usage = {};
  assert(getrusage(RUSAGE_SELF, &usage) == 0);
  return usage.ru_maxrss;
}

int main()
{
  char temp[] = "./build_release/file_sql_scan_XXXXXX";
  const char *directory = mkdtemp(temp);
  assert(directory != nullptr);
  const std::string base(directory);
  const std::string allowed = base + "/allowed";
  assert(mkdir(allowed.c_str(), 0700) == 0);
  char root_buf[PATH_MAX + 1];
  assert(realpath(allowed.c_str(), root_buf) != nullptr);
  const std::string root(root_buf);
  const std::string csv = root + "/safe.csv";
  const std::string outside = base + "/outside.csv";
  {
    std::ofstream out(csv, std::ios::binary);
    out << "id,note\r\n1,\"say \"\"yes\"\"\"\r\n2,\"two\r\nlines\"\r\n";
    std::ofstream other(outside);
    other << "private\n";
  }
  FileCsvSchema schema;
  assert(classify_csv_value("2024-02-29") == FileCsvSchema::Kind::DATE);
  assert(classify_csv_value("2025-02-29") == FileCsvSchema::Kind::VARCHAR);
  assert(classify_csv_value("2026-08-03T12:13:14") == FileCsvSchema::Kind::DATETIME);
  assert(classify_csv_value("2026-08-03 25:13:14") == FileCsvSchema::Kind::VARCHAR);
  assert(infer_csv_schema_fd(open_secure_csv_fd(root, csv), schema) == FileCsvScan::Status::OK);
  assert(schema.rows == 2 && schema.names.size() == 2);
  assert(infer_csv_schema_fd(open_secure_csv_fd(root, csv), schema, false) == FileCsvScan::Status::OK);
  assert(schema.rows == 3 && schema.names[0] == "column0");
  {
    FileCsvScan scan;
    assert(scan.open_fd(open_secure_csv_fd(root, csv)) == FileCsvScan::Status::OK);
    std::vector<std::string> row;
    assert(scan.next(row) == FileCsvScan::Status::OK && row[0] == "id");
    assert(scan.next(row) == FileCsvScan::Status::OK && row[1] == "say \"yes\"");
    assert(scan.next(row) == FileCsvScan::Status::OK && row[1] == "two\r\nlines");
    assert(scan.next(row) == FileCsvScan::Status::END);
    assert(scan.rewind() == FileCsvScan::Status::OK);
    assert(scan.next(row) == FileCsvScan::Status::OK && row[0] == "id");
    std::ofstream edit(csv, std::ios::app);
    edit << "3,changed\n";
    edit.close();
    assert(scan.next(row) == FileCsvScan::Status::CHANGED);
  }
  assert(open_secure_csv_fd(root, outside) < 0);
  const std::string link = root + "/inside_link.csv";
  assert(symlink("safe.csv", link.c_str()) == 0);
  char canonical[PATH_MAX + 1];
  assert(realpath(link.c_str(), canonical) != nullptr);
  const int link_fd = open_secure_csv_fd(root, canonical);
  assert(link_fd >= 0);
  close(link_fd);

  // Simulate replacement after realpath/check but before the open.
  const std::string saved = root + "/saved.csv";
  assert(rename(csv.c_str(), saved.c_str()) == 0);
  assert(symlink(outside.c_str(), csv.c_str()) == 0);
  assert(open_secure_csv_fd(root, canonical) < 0);

  for (const auto count : {100000, 1000000}) {
    const std::string path = root + "/rows_" + std::to_string(count) + ".csv";
    {
      std::ofstream out(path);
      out << "id,value\n";
      for (int i = 0; i < count; ++i) out << i << ',' << i % 1000 << '\n';
    }
    assert(infer_csv_schema_fd(open_secure_csv_fd(root, path), schema) == FileCsvScan::Status::OK);
    assert(schema.rows == static_cast<size_t>(count));
  }
  const long rss_kb = peak_rss_kb();
  assert(rss_kb < 32768);
  std::cout << "csv scan/security PASS; peak RSS " << rss_kb << " KiB\n";

  assert(unlink((root + "/rows_100000.csv").c_str()) == 0);
  assert(unlink((root + "/rows_1000000.csv").c_str()) == 0);
  assert(unlink(csv.c_str()) == 0);
  assert(unlink(saved.c_str()) == 0);
  assert(unlink(link.c_str()) == 0);
  assert(unlink(outside.c_str()) == 0);
  assert(rmdir(root.c_str()) == 0);
  assert(rmdir(base.c_str()) == 0);
}
