"""Native glob/COPY SQL checks on the dedicated seekdb instance (port 2884)."""
import tempfile
import unittest
from pathlib import Path

from test_file_csv_sql import PORT, ROOT, SOCKET, query, quoted


class GlobCopySQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if PORT != "2884" or not Path(SOCKET).exists():
            raise RuntimeError("Only the dedicated seekdb instance on port 2884 is permitted")
        cls.original_root = query("SELECT @@global.secure_file_priv").stdout.strip()
        cls.temp = tempfile.TemporaryDirectory(prefix="glob_copy_sql_", dir=ROOT / "build_release")
        cls.base = Path(cls.temp.name)
        cls.allowed = cls.base / "allowed"
        cls.allowed.mkdir()
        cls.csv = cls.allowed / "input.csv"
        cls.csv.write_text('id,name,score\n1,Alice,8.5\n2,"Bob, Jr.",NULL\n3,"Chen ""C""",9.2\n')
        (cls.allowed / "file_z.csv").write_text("id\n2\n")
        (cls.allowed / "file_a.csv").write_text("id\n1\n")
        cls.outside = cls.base / "outside.csv"
        cls.outside.write_text("id\n99\n")
        (cls.allowed / "outside_link.csv").symlink_to(cls.outside)
        result = query(f"SET GLOBAL secure_file_priv={quoted(cls.allowed)}", socket=True)
        if result.returncode:
            cls.temp.cleanup()
            raise RuntimeError(result.stderr)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            result = query(f"SET GLOBAL secure_file_priv={quoted(cls.original_root)}", socket=True)
            if result.returncode:
                raise RuntimeError(result.stderr)
        finally:
            cls.temp.cleanup()

    def run_file_sql(self, sql: str):
        return query("SET SESSION enable_file_sql=ON; " + sql)

    def test_glob_sorted_empty_and_symlink(self) -> None:
        result = self.run_file_sql(
            f"SELECT substring_index(file, '/', -1) FROM glob({quoted(self.allowed / 'file_*.csv')}) ORDER BY file"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "file_a.csv\nfile_z.csv")
        result = self.run_file_sql(
            f"SELECT count(*) FROM glob({quoted(self.allowed / 'outside_link*.csv')})"
        )
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "0"), result.stderr)
        result = self.run_file_sql(f"SELECT count(*) FROM glob({quoted(self.allowed / 'missing*.csv')})")
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "0"), result.stderr)

    def test_copy_header_quote_null_and_readback(self) -> None:
        output = self.allowed / "export.csv"
        result = self.run_file_sql(
            f"COPY (SELECT id,name,score FROM read_csv({quoted(self.csv)}) ORDER BY id) "
            f"TO {quoted(output)} (format csv, header, nullstr 'NULL')"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text().splitlines()[0], '"id","name","score"')
        result = self.run_file_sql(f"SELECT id,name,score FROM read_csv({quoted(output)}) ORDER BY id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "1\tAlice\t8.5\n2\tBob, Jr.\tNULL\n3\tChen \"C\"\t9.2")
        before = output.read_bytes()
        result = self.run_file_sql(
            f"COPY (SELECT id FROM read_csv({quoted(self.csv)})) "
            f"TO {quoted(output)} (format csv, header, nullstr 'NULL')"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_bytes(), before)

    def test_default_off_and_secure_boundary(self) -> None:
        disabled = query(f"SELECT count(*) FROM glob({quoted(self.allowed / '*.csv')})")
        self.assertNotEqual(disabled.returncode, 0)
        self.assertIn("ERROR 1235", disabled.stderr)
        disabled_copy = query(
            f"COPY (SELECT 1) TO {quoted(self.allowed / 'off.csv')} "
            "(format csv, header, nullstr 'NULL')"
        )
        self.assertNotEqual(disabled_copy.returncode, 0)
        self.assertFalse((self.allowed / "off.csv").exists())
        outside_glob = self.run_file_sql(f"SELECT * FROM glob({quoted(self.base / '*.csv')})")
        self.assertNotEqual(outside_glob.returncode, 0)
        outside_copy = self.run_file_sql(
            f"COPY (SELECT 1) TO {quoted(self.base / 'escape.csv')} "
            "(format csv, header, nullstr 'NULL')"
        )
        self.assertNotEqual(outside_copy.returncode, 0)
        self.assertFalse((self.base / "escape.csv").exists())

    def test_glob_filter_limit_and_rescan(self) -> None:
        result = self.run_file_sql(
            f"SELECT substring_index(file, '/', -1) FROM glob({quoted(self.allowed / 'file_*.csv')}) "
            "WHERE file LIKE '%file_z.csv' LIMIT 1"
        )
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "file_z.csv"), result.stderr)
        result = self.run_file_sql(
            f"SELECT count(*) FROM glob({quoted(self.allowed / 'file_*.csv')}) a "
            f"JOIN glob({quoted(self.allowed / 'file_*.csv')}) b ON a.file=b.file"
        )
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "2"), result.stderr)

    def test_copy_symlink_and_traversal_rejected(self) -> None:
        link = self.allowed / "target_link.csv"
        link.symlink_to(self.outside)
        original = self.outside.read_bytes()
        result = self.run_file_sql(
            f"COPY (SELECT id FROM read_csv({quoted(self.csv)})) TO {quoted(link)} "
            "(format csv, header, nullstr 'NULL')"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.outside.read_bytes(), original)
        parent_link = self.allowed / "outdir"
        parent_link.symlink_to(self.base, target_is_directory=True)
        for path in (parent_link / "escape.csv", self.allowed / ".." / "escape.csv"):
            result = self.run_file_sql(
                f"COPY (SELECT 1) TO {quoted(path)} (format csv, header, nullstr 'NULL')"
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((self.base / "escape.csv").exists())
        result = self.run_file_sql(f"SELECT * FROM glob({quoted(self.allowed / '..' / '*.csv')})")
        self.assertNotEqual(result.returncode, 0)
        result = self.run_file_sql(f"SELECT * FROM glob({quoted(parent_link / '*.csv')})")
        self.assertNotEqual(result.returncode, 0)

if __name__ == "__main__":
    unittest.main()
