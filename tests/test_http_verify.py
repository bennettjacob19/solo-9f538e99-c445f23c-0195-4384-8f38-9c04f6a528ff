"""HTTP 层核验接口测试（TestClient + 临时 SQLite，不依赖 PostgreSQL）。

通过把捕获线程的 start/stop 替换为空操作，直接在真实路由上验证：
/api/points、/api/directory、/api/verify、/api/history 的序列化与可信标记。

python3 -m unittest tests.test_http_verify -v
"""
import os
import tempfile
import unittest

_tmp = tempfile.mkdtemp(prefix="http-seal-")
os.environ["SQLITE_PATH"] = os.path.join(_tmp, "history.db")
os.environ.setdefault("PG_DSN", "host=127.0.0.1 port=1 dbname=x user=x")

from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402
from app import seal  # noqa: E402

SHA = "a" * 64


class HttpVerifyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 不启动真实捕获线程
        main.worker.start = lambda: None
        main.worker.stop = lambda timeout=10.0: None
        cls.client = TestClient(main.app)
        cls.client.__enter__()
        store = main.store
        store.mark_initializing("slot")
        store.save_baseline(
            [(1001, "billing", "1.0", SHA), (1002, "risk", "2.0", SHA)],
            "0/1000",
        )
        store.mark_streaming()
        store.persist_transaction(
            5001, "0/2000", None,
            [
                {"artifact_no": 1003, "change_type": "INSERT",
                 "service_name": "cart", "version": "3", "sha256": SHA},
                {"artifact_no": 1001, "change_type": "UPDATE",
                 "service_name": "billing", "version": "1.1", "sha256": SHA},
            ],
        )
        store.persist_transaction(
            5002, "0/3000", None,
            [{"artifact_no": 1002, "change_type": "DELETE",
              "service_name": None, "version": None, "sha256": None}],
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)
        main.store.close()

    def test_points_carry_seal_seq(self):
        r = self.client.get("/api/points")
        self.assertEqual(r.status_code, 200)
        seqs = [p["seal_seq"] for p in r.json()["points"]]
        self.assertEqual(seqs, [0, 1, 2])

    def test_verify_latest_verified_and_trusted(self):
        r = self.client.get("/api/verify")
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["status"], seal.VERIFIED)
        self.assertEqual(body["checked_seals"], 3)
        self.assertFalse(body["seal_genesis"]["legacy_upgrade"])
        # 1002 已删除，目录剩 1001/1003，均可信
        nos = {a["artifact_no"]: a["trust"] for a in body["artifacts"]}
        self.assertEqual(nos, {1001: "TRUSTED", 1003: "TRUSTED"})

    def test_verify_at_point_truncates(self):
        r = self.client.get("/api/verify", params={"point": "0/2000"})
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], seal.VERIFIED)
        self.assertEqual(body["checked_seals"], 2)  # genesis + txn1

    def test_verify_bad_lsn_400_unknown_point_404(self):
        self.assertEqual(self.client.get("/api/verify", params={"point": "nope"}).status_code, 400)
        self.assertEqual(self.client.get("/api/verify", params={"point": "0/FFFF"}).status_code, 404)

    def test_history_trust_labels(self):
        r = self.client.get("/api/history/1001")
        self.assertEqual(r.status_code, 200)
        trusts = [h["trust"] for h in r.json()["history"]]
        self.assertEqual(trusts, ["TRUSTED", "TRUSTED"])

    def test_tamper_marks_untrusted_via_http(self):
        store = main.store
        with store._conn:
            store._conn.execute(
                "UPDATE artifact_history SET version='HACK' WHERE seal_seq=1"
            )
        try:
            r = self.client.get("/api/verify")
            body = r.json()
            self.assertEqual(body["status"], seal.INVALID)
            self.assertEqual(body["first_invalid_seq"], 1)
            self.assertEqual(body["first_invalid_reason"], seal.REASON_ENTRY_HASH)
            trusts = {a["artifact_no"]: a["trust"] for a in body["artifacts"]}
            # seq1 及之后不可信；genesis(seq0) 行仍可信
            self.assertEqual(trusts[1003], "UNTRUSTED")
            # 1001 当前值来自 seq1 的 UPDATE，也不可信
            self.assertEqual(trusts[1001], "UNTRUSTED")
            # 进度卡轻量链状态：entry_hash 未改但行被改 -> 链链接本身仍 OK，
            # 完整核验 /api/verify 才暴露改值（符合设计分工）
            self.assertTrue(self.client.get("/api/progress").json()["seal"]["link_ok"])
        finally:
            # 还原为封存时的原始值，恢复链完整性，避免用例间状态污染
            with store._conn:
                store._conn.execute(
                    "UPDATE artifact_history SET version=CASE artifact_no"
                    " WHEN 1003 THEN '3' WHEN 1001 THEN '1.1' END"
                    " WHERE seal_seq=1 AND version='HACK'"
                )
            self.assertEqual(self.client.get("/api/verify").json()["status"],
                             seal.VERIFIED)


if __name__ == "__main__":
    unittest.main()
