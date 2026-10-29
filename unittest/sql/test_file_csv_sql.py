"""Real seekdb CSV SQL smoke tests against a dedicated local instance on port 2884."""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PORT = os.environ.get("FILE_SQL_TEST_PORT", "2884")
SOCKET = os.environ.get(
    "FILE_SQL_TEST_SOCKET",
    str(ROOT / "build_release/file_sql_instance_2/run/sql.sock"),
)


def query(sql: str, *, socket: bool = False) -> subprocess.CompletedProcess[str]:
    args = ["mysql", "--no-defaults", "--batch", "--raw", "--skip-column-names"]
    if socket:
        args += ["--protocol=SOCKET", f"--socket={SOCKET}"]
    else:
        args += ["--protocol=TCP", "-h127.0.0.1", f"-P{PORT}"]
    return subprocess.run(args + ["-uroot", "-Dtest", "-e", sql], text=True, capture_output=True)


def quoted(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class CSVSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if PORT != "2884" or not Path(SOCKET).exists():
            raise RuntimeError("Only the dedicated seekdb instance on port 2884 is permitted")
        cls.original_root = query("SELECT @@global.secure_file_priv").stdout.strip()
        if not cls.original_root:
            raise RuntimeError("Failed to read secure_file_priv from isolated seekdb")
        cls.temp = tempfile.TemporaryDirectory(prefix="csv_sql_", dir=ROOT / "build_release")
        cls.base = Path(cls.temp.name)
        cls.allowed = cls.base / "allowed"
        cls.allowed.mkdir()
        cls.csv = cls.allowed / "rows.csv"
        cls.csv.write_bytes(
            b'id,group_name,amount,note,nullable,flag\r\n'
            b'1,A,10.5,"one, two",NULL,true\r\n'
            b'2,A,2.5,"line1\r\nline2",,false\r\n'
            b'3,B,7,"say ""yes""",ok,true\r\n'
        )
        cls.bad = cls.allowed / "bad.csv"
        cls.bad.write_text("id,value\n1,ok\n2\n")
        cls.no_header = cls.allowed / "no_header.csv"
        cls.no_header.write_text("1,first\n2,second\n")
        cls.dates = cls.allowed / "dates.csv"
        cls.dates.write_text(
            "id,day,created\n"
            "1,2024-02-29,2024-02-29T12:13:14\n"
            "2,2024-03-01,2024-03-01 08:00:00\n"
        )
        cls.fractional_dates = cls.allowed / "fractional_dates.csv"
        cls.fractional_dates.write_text("created\n2024-02-29 12:13:14.123\n")
        cls.outside = cls.base / "outside.csv"
        cls.outside.write_text("id,value\n1,private\n")
        (cls.allowed / "outside_link.csv").symlink_to(cls.outside)
        (cls.allowed / "inside_link.csv").symlink_to(cls.csv)
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

    def test_default_off(self) -> None:
        result = query(f"SELECT COUNT(*) FROM read_csv({quoted(self.csv)})")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ERROR 1235", result.stderr)

    def test_columns_quotes_null_and_filter(self) -> None:
        self.assert_rows(
            f"SELECT id,HEX(note),nullable,flag FROM read_csv({quoted(self.csv)}) "
            "WHERE amount>=7 ORDER BY id LIMIT 2",
            "1\t6F6E652C2074776F\tNULL\t1\n"
            "3\t736179202279657322\tok\t1",
        )
        self.assert_rows(
            f"SELECT HEX(note) FROM read_csv({quoted(self.csv)}) WHERE id=2",
            "6C696E65310D0A6C696E6532",
        )

    def test_all_columns_sort(self) -> None:
        self.assert_rows(
            f"SELECT id,group_name,amount,HEX(note),nullable,flag "
            f"FROM read_csv({quoted(self.csv)}) ORDER BY id",
            "1\tA\t10.5\t6F6E652C2074776F\tNULL\t1\n"
            "2\tA\t2.5\t6C696E65310D0A6C696E6532\tNULL\t0\n"
            "3\tB\t7\t736179202279657322\tok\t1",
        )

    def test_group_having_and_rescan(self) -> None:
        sql = (f"SELECT group_name,COUNT(*),SUM(amount) FROM read_csv({quoted(self.csv)}) "
               "GROUP BY group_name HAVING SUM(amount)>10 ORDER BY group_name")
        self.assert_rows(sql, "A\t2\t13")
        self.assert_rows(sql, "A\t2\t13")

    def test_options_and_dates(self) -> None:
        self.assert_rows(
            f"SELECT id,day,created FROM read_csv({quoted(self.dates)}, "
            "header=true,sample_size=-1) ORDER BY id",
            "1\t2024-02-29\t2024-02-29 12:13:14\n"
            "2\t2024-03-01\t2024-03-01 08:00:00",
        )
        self.assert_rows(
            f"SELECT column0,column1 FROM read_csv({quoted(self.no_header)}, "
            "header=false) ORDER BY column0",
            "1\tfirst\n2\tsecond",
        )
        self.assert_rows(
            f"SELECT created FROM read_csv({quoted(self.fractional_dates)})",
            "2024-02-29 12:13:14.123",
        )
        unsupported = query(
            f"SET SESSION enable_file_sql=ON; "
            f"SELECT COUNT(*) FROM read_csv({quoted(self.csv)},sample_size=10)"
        )
        self.assertIn("ERROR 1235", unsupported.stderr)

    def test_describe_csv_metadata(self) -> None:
        self.assert_rows(
            f"DESCRIBE SELECT * FROM read_csv({quoted(self.csv)})",
            "id\tBIGINT\tNO\tNULL\tNULL\tNULL\n"
            "group_name\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
            "amount\tDOUBLE\tNO\tNULL\tNULL\tNULL\n"
            "note\tVARCHAR\tNO\tNULL\tNULL\tNULL\n"
            "nullable\tVARCHAR\tYES\tNULL\tNULL\tNULL\n"
            "flag\tBOOLEAN\tNO\tNULL\tNULL\tNULL",
        )
        plan = query(f"SET SESSION enable_file_sql=ON; EXPLAIN SELECT * "
                     f"FROM read_csv({quoted(self.csv)})")
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertIn("FUNCTION_TABLE", plan.stdout)
        for path, enabled in ((self.csv, False), (self.outside, True)):
            prefix = "SET SESSION enable_file_sql=ON; " if enabled else ""
            denied = query(prefix + f"DESCRIBE SELECT * FROM read_csv({quoted(path)})")
            self.assertNotEqual(denied.returncode, 0)
            self.assertIn("ERROR 1227" if enabled else "ERROR 1235", denied.stderr)

    def test_csv_joins(self) -> None:
        self.assert_rows(
            f"SELECT a.id,b.group_name FROM read_csv({quoted(self.csv)}) a "
            f"JOIN read_csv({quoted(self.csv)}) b ON a.id=b.id "
            "WHERE a.id=2 ORDER BY a.id",
            "2\tA",
        )
        try:
            result = query(
                "SET SESSION enable_file_sql=ON; "
                "CREATE TABLE IF NOT EXISTS file_csv_join_native_case (id BIGINT, label VARCHAR(20)); "
                "DELETE FROM file_csv_join_native_case; "
                "INSERT INTO file_csv_join_native_case VALUES (1,'one'),(2,'two'),(NULL,'missing'); "
                f"SELECT f.id,t.label FROM read_csv({quoted(self.csv)}) f "
                "JOIN file_csv_join_native_case t ON f.id=t.id ORDER BY f.id"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "1\tone\n2\ttwo")
        finally:
            query("DROP TABLE IF EXISTS file_csv_join_native_case")

    def test_bad_row_is_atomic(self) -> None:
        result = query(f"SET SESSION enable_file_sql=ON; SELECT * FROM read_csv({quoted(self.bad)})")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ERROR 1210", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_path_boundary_and_symlinks(self) -> None:
        self.assert_rows(
            f"SELECT COUNT(*) FROM read_csv({quoted(self.allowed / 'inside_link.csv')})", "3"
        )
        for path in (self.outside, self.allowed / "outside_link.csv",
                     self.allowed / "../outside.csv"):
            result = query(f"SET SESSION enable_file_sql=ON; "
                           f"SELECT COUNT(*) FROM read_csv({quoted(path)})")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("ERROR 1227", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
