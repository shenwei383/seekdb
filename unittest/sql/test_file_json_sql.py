"""Native read_json/read_ndjson SQL regression against the dedicated seekdb on 2884."""
import tempfile
import unittest
from pathlib import Path

from test_file_csv_sql import PORT, ROOT, SOCKET, query, quoted


class JSONSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if PORT != "2884" or not Path(SOCKET).exists():
            raise RuntimeError("Only the dedicated seekdb instance on port 2884 is permitted")
        cls.original_root = query("SELECT @@global.secure_file_priv").stdout.strip()
        cls.temp = tempfile.TemporaryDirectory(prefix="json_sql_", dir=ROOT / "build_release")
        cls.base = Path(cls.temp.name)
        cls.allowed = cls.base / "allowed"
        cls.allowed.mkdir()
        cls.jsonl = cls.allowed / "rows.jsonl"
        cls.jsonl.write_bytes(
            b'{"id":1,"name":"Alice","score":98.5,"active":true,'
            b'"token":"NULL","extra":{"a":1}}\r\n'
            b'{"id":2,"name":"Bob","active":false,"day":"2026-08-02",'
            b'"token":null}\n'
            b'{"id":3,"name":"Chen","score":90,"active":true,'
            b'"note":"ok","token":"","tags":[1,2]}\n'
        )
        cls.ndjson = cls.allowed / "rows.ndjson"
        cls.ndjson.write_bytes(cls.jsonl.read_bytes())
        cls.csv = cls.allowed / "rows.csv"
        cls.csv.write_text("id,label\n1,first\n2,second\n3,third\n")
        cls.bad = cls.allowed / "bad.jsonl"
        cls.bad.write_text('{"id":1}\n{"id":\n')
        cls.duplicate = cls.allowed / "duplicate.jsonl"
        cls.duplicate.write_text('{"id":1,"id":2}\n')
        cls.empty = cls.allowed / "empty.jsonl"
        cls.empty.write_text("")
        cls.unicode = cls.allowed / "unicode.jsonl"
        cls.unicode.write_bytes(
            b"\xef\xbb\xbf" +
            '{"id":1,"mixed":1,"label":"雪"}\r\n\n'
            '{"id":2,"mixed":"two","label":"猫"}\n'.encode("utf-8")
        )
        cls.invalid_utf8 = cls.allowed / "invalid_utf8.jsonl"
        cls.invalid_utf8.write_bytes(b'{"label":"\xff"}\n')
        cls.array_root = cls.allowed / "array_root.jsonl"
        cls.array_root.write_text("[1,2]\n")
        cls.outside = cls.base / "outside.jsonl"
        cls.outside.write_text('{"secret":"private"}\n')
        (cls.allowed / "outside_link.jsonl").symlink_to(cls.outside)
        changed = query(f"SET GLOBAL secure_file_priv={quoted(cls.allowed)}", socket=True)
        if changed.returncode:
            cls.temp.cleanup()
            raise RuntimeError(changed.stderr)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            restored = query(
                f"SET GLOBAL secure_file_priv={quoted(cls.original_root)}", socket=True
            )
            if restored.returncode:
                raise RuntimeError(restored.stderr)
        finally:
            cls.temp.cleanup()

    def assert_rows(self, sql: str, expected: str) -> None:
        result = query("SET SESSION enable_file_sql=ON; " + sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), expected)

    def test_union_schema_and_nulls(self) -> None:
        self.assert_rows(
            f"SELECT id,name,score,active,day,note FROM read_json({quoted(self.jsonl)}, "
            "format='newline_delimited') ORDER BY id",
            "1\tAlice\t98.5\t1\tNULL\tNULL\n"
            "2\tBob\tNULL\t0\t2026-08-02\tNULL\n"
            "3\tChen\t90\t1\tNULL\tok",
        )
        self.assert_rows(
            f"SELECT id,IFNULL(CONCAT('[',HEX(token),']'),'NULL') "
            f"FROM read_ndjson({quoted(self.ndjson)}) ORDER BY id",
            "1\t[4E554C4C]\n2\tNULL\n3\t[]",
        )

    def test_unicode_bom_and_type_conflict(self) -> None:
        self.assert_rows(
            f"SELECT id,mixed,HEX(label) FROM read_json({quoted(self.unicode)}) "
            "ORDER BY id",
            "1\t1\tE99BAA\n2\ttwo\tE78CAB",
        )

    def test_select_filter_aggregate_and_rescan(self) -> None:
        sql = (f"SELECT active,COUNT(*),SUM(id) FROM read_json({quoted(self.jsonl)}) "
               "WHERE id>=2 GROUP BY active HAVING SUM(id)>1 ORDER BY active")
        self.assert_rows(sql, "0\t1\t2\n1\t1\t3")
        self.assert_rows(sql, "0\t1\t2\n1\t1\t3")
        self.assert_rows(
            f"SELECT id,name FROM read_ndjson({quoted(self.ndjson)}) j "
            "WHERE id>1 ORDER BY id DESC LIMIT 1",
            "3\tChen",
        )
        self.assert_rows(
            f"SELECT extra,tags FROM read_json({quoted(self.jsonl)}) WHERE id=1",
            '{"a":1}\tNULL',
        )
        self.assert_rows(
            f"SELECT extra,tags FROM read_json({quoted(self.jsonl)}) WHERE id=3",
            'NULL\t[1,2]',
        )

    def test_describe(self) -> None:
        self.assert_rows(
            f"DESCRIBE SELECT * FROM read_json({quoted(self.jsonl)})",
            "id\tBIGINT\tNO\tNULL\tNULL\tNULL\n"
            "name\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
            "score\tDOUBLE\tYES\tNULL\tNULL\tNULL\n"
            "active\tBOOLEAN\tNO\tNULL\tNULL\tNULL\n"
            "token\tVARCHAR\tYES\tNULL\tNULL\tNULL\n"
            "extra\tVARCHAR\tYES\tNULL\tNULL\tNULL\n"
            "day\tDATE\tYES\tNULL\tNULL\tNULL\n"
            "note\tVARCHAR\tYES\tNULL\tNULL\tNULL\n"
            "tags\tVARCHAR\tYES\tNULL\tNULL\tNULL",
        )

    def test_json_csv_and_native_joins(self) -> None:
        self.assert_rows(
            f"SELECT j.id,c.label FROM read_json({quoted(self.jsonl)}) j "
            f"JOIN read_csv({quoted(self.csv)}) c ON j.id=c.id "
            "WHERE j.id>1 ORDER BY j.id",
            "2\tsecond\n3\tthird",
        )
        try:
            result = query(
                "SET SESSION enable_file_sql=ON; "
                "CREATE TABLE IF NOT EXISTS file_json_join_native_case "
                "(id BIGINT,label VARCHAR(20)); "
                "DELETE FROM file_json_join_native_case; "
                "INSERT INTO file_json_join_native_case VALUES (1,'one'),(3,'three'),(NULL,'x'); "
                f"SELECT j.id,t.label FROM read_json({quoted(self.jsonl)}) j "
                "JOIN file_json_join_native_case t ON j.id=t.id ORDER BY j.id"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "1\tone\n3\tthree")
        finally:
            query("DROP TABLE IF EXISTS file_json_join_native_case")

    def test_native_table_equivalence(self) -> None:
        try:
            prepared = query(
                "CREATE TABLE IF NOT EXISTS file_json_equivalence_case "
                "(id BIGINT,score DOUBLE,active TINYINT,day DATE); "
                "DELETE FROM file_json_equivalence_case; "
                "INSERT INTO file_json_equivalence_case VALUES "
                "(1,98.5,1,NULL),(2,NULL,0,'2026-08-02'),(3,90,1,NULL)"
            )
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            expected = query(
                "SELECT id,score,active,day FROM file_json_equivalence_case ORDER BY id"
            )
            actual = query(
                "SET SESSION enable_file_sql=ON; "
                f"SELECT id,score,active,day FROM read_json({quoted(self.jsonl)}) ORDER BY id"
            )
            self.assertEqual(expected.returncode, 0, expected.stderr)
            self.assertEqual(actual.returncode, 0, actual.stderr)
            self.assertEqual(actual.stdout, expected.stdout)
        finally:
            query("DROP TABLE IF EXISTS file_json_equivalence_case")

    def test_million_row_sql(self) -> None:
        large = self.allowed / "million.jsonl"
        try:
            with large.open("w") as out:
                for index in range(1_000_000):
                    out.write(f'{{"id":{index},"value":{index % 1000}}}\n')
            self.assert_rows(
                f"SELECT COUNT(*),SUM(id) FROM read_json({quoted(large)})",
                "1000000\t499999500000",
            )
        finally:
            large.unlink(missing_ok=True)

    def test_invalid_and_security(self) -> None:
        disabled = query(f"SELECT COUNT(*) FROM read_json({quoted(self.jsonl)})")
        self.assertIn("ERROR 1235", disabled.stderr)
        for path in (self.bad, self.duplicate, self.empty,
                     self.invalid_utf8, self.array_root):
            invalid = query(
                f"SET SESSION enable_file_sql=ON; SELECT * FROM read_json({quoted(path)})"
            )
            self.assertNotEqual(invalid.returncode, 0)
            self.assertIn("ERROR 1210", invalid.stderr)
            self.assertEqual(invalid.stdout, "")
        for path in (self.outside, self.allowed / "outside_link.jsonl",
                     self.allowed / "../outside.jsonl"):
            denied = query(f"SET SESSION enable_file_sql=ON; "
                           f"SELECT COUNT(*) FROM read_json({quoted(path)})")
            self.assertNotEqual(denied.returncode, 0)
            self.assertIn("ERROR 1227", denied.stderr)
        unsupported = query(f"SET SESSION enable_file_sql=ON; "
                            f"SELECT * FROM read_json({quoted(self.jsonl)},format='xml')")
        self.assertNotEqual(unsupported.returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
