"""Native Parquet SQL checks on the dedicated seekdb instance (port 2884).

The tiny fixture writer emits flat, uncompressed Parquet V2 pages. It is test
data generation only; the database reads the resulting file without Python.
"""
import struct
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from test_file_csv_sql import PORT, ROOT, SOCKET, query, quoted


def var(number):
    result = bytearray()
    while number >= 128:
        result.append((number & 127) | 128)
        number >>= 7
    result.append(number)
    return bytes(result)


def zig(number):
    return var((number << 1) ^ (number >> 63))


def item(kind, value):
    if kind in (1, 2):
        return b""
    if kind in (4, 5, 6):
        return zig(value)
    if kind == 8:
        data = value.encode() if isinstance(value, str) else value
        return var(len(data)) + data
    if kind == 12:
        return record(value)
    if kind == 9:
        subtype, values = value
        header = bytes([(len(values) << 4) | subtype]) if len(values) < 15 else bytes([0xF0 | subtype]) + var(len(values))
        return header + b"".join(item(subtype, val) for val in values)
    raise ValueError(kind)


def record(fields):
    output = bytearray()
    previous = 0
    for field_id, (kind, value) in sorted(fields.items()):
        delta = field_id - previous
        output.append((delta << 4) | kind if delta < 16 else kind)
        if delta >= 16:
            output += zig(field_id)
        output += item(kind, value)
        previous = field_id
    return bytes(output) + b"\x00"


def snappy_literal(data):
    length = len(data) - 1
    if length < 60:
        prefix = bytes([length << 2])
    else:
        count = max(1, (length.bit_length() + 7) // 8)
        prefix = bytes([(59 + count) << 2]) + length.to_bytes(count, "little")
    return var(len(data)) + prefix + data


def parquet_bytes(columns, rows, group_size=1024, page_version=2, codec=0):
    """columns: (name, physical type, optional, converted type or None)."""
    schema = [{4: (8, "root"), 5: (5, len(columns))}]
    for name, physical, optional, logical in columns:
        node = {1: (5, physical), 3: (5, int(optional)), 4: (8, name)}
        if logical == "modern_string":
            node[10] = (12, {1: (12, {})})
        elif logical is not None:
            node[6] = (5, logical)
        schema.append(node)
    output = bytearray(b"PAR1")
    groups = []
    for start in range(0, len(rows), group_size):
        group_rows = rows[start : start + group_size]
        chunks = []
        for col, (_, physical, optional, _) in enumerate(columns):
            values = [row[col] for row in group_rows]
            levels = b""
            if optional:
                # RLE hybrid bit-packed groups; encode all rows, including >8.
                bits = bytes(
                    sum((1 << bit) for bit in range(8)
                        if group * 8 + bit < len(values) and values[group * 8 + bit] is not None)
                    for group in range((len(values) + 7) // 8)
                )
                levels = var(((len(values) + 7) // 8) * 2 + 1) + bits
            present = [value for value in values if value is not None]
            if physical == 0:
                data = bytes(
                    sum((1 << bit) for bit in range(8)
                        if group * 8 + bit < len(present) and present[group * 8 + bit])
                    for group in range((len(present) + 7) // 8)
                )
            elif physical == 1:
                data = b"".join(struct.pack("<i", value) for value in present)
            elif physical == 2:
                data = b"".join(struct.pack("<q", value) for value in present)
            elif physical == 4:
                data = b"".join(struct.pack("<f", value) for value in present)
            elif physical == 5:
                data = b"".join(struct.pack("<d", value) for value in present)
            elif physical == 6:
                data = b"".join(struct.pack("<I", len(value.encode())) + value.encode()
                                for value in present)
            else:
                raise ValueError(physical)
            if page_version == 2:
                body = levels + (snappy_literal(data) if codec else data)
                uncompressed_size = len(levels) + len(data)
                header = record({
                    1: (5, 3), 2: (5, uncompressed_size), 3: (5, len(body)),
                    8: (12, {1: (5, len(values)), 2: (5, len(values) - len(present)),
                             3: (5, len(values)), 4: (5, 0), 5: (5, len(levels)),
                             6: (5, 0), 7: (1 if codec else 2, bool(codec))}),
                })
            elif page_version == 1:
                raw = (struct.pack("<I", len(levels)) + levels if optional else b"") + data
                body = snappy_literal(raw) if codec else raw
                header = record({
                    1: (5, 0), 2: (5, len(raw)), 3: (5, len(body)),
                    5: (12, {1: (5, len(values)), 2: (5, 0),
                             3: (5, 3), 4: (5, 3)}),
                })
            else:
                raise ValueError(page_version)
            offset = len(output)
            output += header + body
            size = len(header) + len(body)
            meta = {1: (5, physical), 2: (9, (5, [0, 3])),
                    3: (9, (8, [columns[col][0]])), 4: (5, codec),
                    5: (6, len(values)), 6: (6, size - len(body) + uncompressed_size if page_version == 2 else len(header) + len(raw)), 7: (6, size),
                    9: (6, offset)}
            chunks.append({2: (6, offset), 3: (12, meta)})
        groups.append({1: (9, (12, chunks)), 2: (6, len(output) - 4),
                       3: (6, len(group_rows))})
    metadata = record({1: (5, 2), 2: (9, (12, schema)),
                       3: (6, len(rows)), 4: (9, (12, groups))})
    return bytes(output) + metadata + struct.pack("<I", len(metadata)) + b"PAR1"


def dictionary_parquet_bytes():
    """A flat UTF-8 column with a PLAIN dictionary and V2 RLE_DICTIONARY page."""
    output = bytearray(b"PAR1")
    dictionary = ["apple", "banana"]
    dict_body = b"".join(struct.pack("<I", len(s)) + s.encode() for s in dictionary)
    dict_header = record({1: (5, 2), 2: (5, len(dict_body)),
                          3: (5, len(dict_body)),
                          7: (12, {1: (5, len(dictionary)), 2: (5, 0)})})
    dict_offset = len(output)
    output += dict_header + dict_body
    data_offset = len(output)
    # Width 1; one bit-packed group for [0, 1, 1, 0].
    data = bytes([1, 3, 0b0110])
    data_header = record({1: (5, 3), 2: (5, len(data)),
                          3: (5, len(data)),
                          8: (12, {1: (5, 4), 2: (5, 0), 3: (5, 4),
                                   4: (5, 8), 5: (5, 0), 6: (5, 0), 7: (2, False)})})
    output += data_header + data
    size = len(output) - dict_offset
    meta = {1: (5, 6), 2: (9, (5, [0, 3, 8])),
            3: (9, (8, ["fruit"])), 4: (5, 0), 5: (6, 4),
            6: (6, size), 7: (6, size), 9: (6, data_offset),
            11: (6, dict_offset)}
    schema = [{4: (8, "root"), 5: (5, 1)},
              {1: (5, 6), 3: (5, 0), 4: (8, "fruit"), 6: (5, 0)}]
    group = {1: (9, (12, [{2: (6, dict_offset), 3: (12, meta)}])),
             2: (6, size), 3: (6, 4)}
    metadata = record({1: (5, 2), 2: (9, (12, schema)),
                       3: (6, 4), 4: (9, (12, [group]))})
    return bytes(output) + metadata + struct.pack("<I", len(metadata)) + b"PAR1"


class ParquetSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if PORT != "2884" or not Path(SOCKET).exists():
            raise RuntimeError("Dedicated seekdb on port 2884 is required")
        cls.old_root = query("SELECT @@global.secure_file_priv").stdout.strip()
        cls.temp = tempfile.TemporaryDirectory(prefix="parquet_sql_", dir=ROOT / "build_release")
        cls.base = Path(cls.temp.name)
        cls.allowed = cls.base / "allowed"
        cls.allowed.mkdir()
        cls.parquet = cls.allowed / "rows.parquet"
        cls.parquet.write_bytes(parquet_bytes(
            [("id", 2, False, None), ("name", 6, False, 0),
             ("age", 1, True, None), ("score", 5, True, None),
             ("active", 0, True, None), ("day", 1, True, 6)],
            [(1, "Alice", 30, 9.5, True, 20666),
             (2, "Bob", None, 7.0, False, None),
             (3, "雪", 40, None, None, 20668)], group_size=2))
        cls.csv = cls.allowed / "join.csv"
        cls.csv.write_text("id,label\n1,first\n3,third\n")
        cls.jsonl = cls.allowed / "join.jsonl"
        cls.jsonl.write_text('{"id":1,"label":"one"}\n{"id":2,"label":"two"}\n')
        cls.outside = cls.base / "outside.parquet"
        cls.outside.write_bytes(cls.parquet.read_bytes())
        (cls.allowed / "link.parquet").symlink_to(cls.outside)
        result = query(f"SET GLOBAL secure_file_priv={quoted(cls.allowed)}", socket=True)
        if result.returncode:
            cls.temp.cleanup()
            raise RuntimeError(result.stderr)

    @classmethod
    def tearDownClass(cls):
        try:
            restored = query(f"SET GLOBAL secure_file_priv={quoted(cls.old_root)}", socket=True)
            if restored.returncode:
                raise RuntimeError(restored.stderr)
        finally:
            cls.temp.cleanup()

    def assert_sql(self, sql, expected):
        result = query("SET SESSION enable_file_sql=ON; " + sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), expected)

    def test_project_null_and_types(self):
        self.assert_sql(f"SELECT id,name,age,score,active,day FROM read_parquet({quoted(self.parquet)}) ORDER BY id",
                        "1\tAlice\t30\t9.5\t1\t2026-08-01\n"
                        "2\tBob\tNULL\t7\t0\tNULL\n"
                        "3\t雪\t40\tNULL\tNULL\t2026-08-03")

    def test_describe_and_explain(self):
        self.assert_sql(f"DESCRIBE SELECT * FROM read_parquet({quoted(self.parquet)})",
                        "id\tBIGINT\tNO\tNULL\tNULL\tNULL\n"
                        "name\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
                        "age\tBIGINT\tYES\tNULL\tNULL\tNULL\n"
                        "score\tDOUBLE\tYES\tNULL\tNULL\tNULL\n"
                        "active\tBOOLEAN\tYES\tNULL\tNULL\tNULL\n"
                        "day\tDATE\tYES\tNULL\tNULL\tNULL")
        plan = query("SET SESSION enable_file_sql=ON; " +
                     f"EXPLAIN SELECT * FROM read_parquet({quoted(self.parquet)})")
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertIn("FUNCTION_TABLE", plan.stdout)
        self.assertIn("read_parquet", plan.stdout)

    def test_filter_group_limit_rescan(self):
        sql = (f"SELECT active,COUNT(*),SUM(id) FROM read_parquet({quoted(self.parquet)}) "
               "WHERE id<=2 GROUP BY active HAVING COUNT(*)=1 ORDER BY active LIMIT 2")
        self.assert_sql(sql, "0\t1\t2\n1\t1\t1")
        self.assert_sql(sql, "0\t1\t2\n1\t1\t1")

    def test_joins(self):
        self.assert_sql(f"SELECT p.id,q.name FROM read_parquet({quoted(self.parquet)}) p "
                        f"JOIN read_parquet({quoted(self.parquet)}) q ON p.id=q.id "
                        "WHERE p.id>1 ORDER BY p.id",
                        "2\tBob\n3\t雪")
        self.assert_sql(f"SELECT p.id,c.label FROM read_parquet({quoted(self.parquet)}) p "
                        f"JOIN read_csv({quoted(self.csv)}) c ON p.id=c.id ORDER BY p.id",
                        "1\tfirst\n3\tthird")
        self.assert_sql(f"SELECT p.id,j.label FROM read_parquet({quoted(self.parquet)}) p "
                        f"JOIN read_json({quoted(self.jsonl)}) j ON p.id=j.id ORDER BY p.id",
                        "1\tone\n2\ttwo")
        try:
            created = query("CREATE TABLE IF NOT EXISTS parquet_join_case (id BIGINT,label VARCHAR(20)); "
                            "DELETE FROM parquet_join_case; INSERT INTO parquet_join_case VALUES (1,'one'),(3,'three')")
            self.assertEqual(created.returncode, 0, created.stderr)
            self.assert_sql(f"SELECT p.id,t.label FROM read_parquet({quoted(self.parquet)}) p "
                            "JOIN parquet_join_case t ON p.id=t.id ORDER BY p.id",
                            "1\tone\n3\tthree")
        finally:
            query("DROP TABLE IF EXISTS parquet_join_case")

    def test_security_and_corruption(self):
        default_off = query(f"SELECT COUNT(*) FROM read_parquet({quoted(self.parquet)})")
        self.assertNotEqual(default_off.returncode, 0)
        for path in (self.outside, self.allowed / "link.parquet"):
            denied = query("SET SESSION enable_file_sql=ON; " +
                           f"SELECT * FROM read_parquet({quoted(path)})")
            self.assertNotEqual(denied.returncode, 0, path)
        bad = self.allowed / "truncated.parquet"
        bad.write_bytes(self.parquet.read_bytes()[:-7])
        rejected = query("SET SESSION enable_file_sql=ON; " +
                         f"SELECT * FROM read_parquet({quoted(bad)})")
        self.assertNotEqual(rejected.returncode, 0)
        broken_page = self.allowed / "broken_page.parquet"
        bytes_ = bytearray(self.parquet.read_bytes())
        bytes_[4] = 0xFF
        broken_page.write_bytes(bytes_)
        rejected = query("SET SESSION enable_file_sql=ON; " +
                         f"SELECT * FROM read_parquet({quoted(broken_page)})")
        self.assertNotEqual(rejected.returncode, 0)

    def test_v1_snappy_float_timestamp(self):
        path = self.allowed / "v1_snappy.parquet"
        instant = int(datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
        path.write_bytes(parquet_bytes(
            [("id", 1, False, None), ("ratio", 4, True, None),
             ("created", 2, False, 9)],
            [(1, 1.25, instant), (2, None, instant + 1000)],
            page_version=1, codec=1))
        self.assert_sql(
            f"SELECT id,ratio,DATE_FORMAT(created,'%Y-%m-%d %H:%i:%s') "
            f"FROM read_parquet({quoted(path)}) ORDER BY id",
            "1\t1.25\t2026-08-01 10:00:00\n"
            "2\tNULL\t2026-08-01 10:00:01",
        )

    def test_modern_string_logical_type(self):
        path = self.allowed / "modern_string.parquet"
        path.write_bytes(parquet_bytes(
            [("label", 6, False, "modern_string")], [("雪",), ("cat",)]))
        self.assert_sql(f"SELECT label FROM read_parquet({quoted(path)}) ORDER BY label",
                        "cat\n雪")

    def test_dictionary_encoding(self):
        path = self.allowed / "dictionary.parquet"
        path.write_bytes(dictionary_parquet_bytes())
        self.assert_sql(f"SELECT fruit FROM read_parquet({quoted(path)})",
                        "apple\nbanana\nbanana\napple")

    def test_unsupported_decimal_rejected(self):
        path = self.allowed / "unsupported_decimal.parquet"
        path.write_bytes(parquet_bytes([("amount", 2, False, 5)], [(123,)]))
        result = query("SET SESSION enable_file_sql=ON; " +
                       f"SELECT * FROM read_parquet({quoted(path)})")
        self.assertNotEqual(result.returncode, 0)

    def test_million_rows(self):
        path = self.allowed / "million.parquet"
        columns = [("id", 2, False, None)]
        # The writer retains the generated bytes for this test; seekdb's
        # scanner itself keeps only one page and metadata per column.
        path.write_bytes(parquet_bytes(columns, [(i,) for i in range(1_000_000)],
                                      group_size=4096))
        self.assert_sql(f"SELECT COUNT(*),SUM(id) FROM read_parquet({quoted(path)})",
                        "1000000\t499999500000")


if __name__ == "__main__":
    unittest.main(verbosity=2)
