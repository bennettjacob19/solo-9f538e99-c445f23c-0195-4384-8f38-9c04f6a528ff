"""存储层 + 连续封存端到端（纯 stdlib，不依赖 fastapi/psycopg2）。

python3 -m unittest tests.test_storage_seal -v

覆盖：
1. 新装库：基线即 GENESIS 封存，每笔事务同事务落盘 + 链式封存；
2. 旧库升级：已有历史在首次启用时补 GENESIS（仅证明此后未变，旧行 UNKNOWN）；
3. 改值 / 缺行 / 乱序 / 删封存 -> verify_at 指出最早失效事务，受影响行 UNTRUSTED；
4. 重放幂等：同一 xid 不重复落盘、不重复封存；
5. 强杀重启等价路径：重建 store 后链完整、续接新事务仍可核验；
6. 落盘失败不推进“确认位置”语义（last_commit_lsn 与数据同事务）。
"""
import os
import sqlite3
import tempfile
import unittest

from app.storage import HistoryStore, lsn_to_num
from app import seal

SHA = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"


def change(no, op="INSERT", **kw):
    base = {
        "artifact_no": no,
        "change_type": op,
        "service_name": f"svc-{no}",
        "version": "1.0.0",
        "sha256": SHA,
    }
    if op == "DELETE":
        base.update(service_name=None, version=None, sha256=None)
    base.update(kw)
    return base


class StorageSealTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="seal-db-")
        self.path = os.path.join(self.tmp, "history.db")

    def _new_store(self):
        return HistoryStore(self.path)

    def _baseline_and_stream(self, store):
        store.mark_initializing("slot")
        store.save_baseline(
            [(1001, "billing", "1.0", SHA), (1002, "risk", "2.0", SHA)],
            "0/1000",
        )
        store.mark_streaming()

    def test_fresh_baseline_creates_genesis_and_chain_verifies(self):
        store = self._new_store()
        self._baseline_and_stream(store)
        seals = store._load_seals_locked()
        self.assertEqual([s["seq"] for s in seals], [0])
        self.assertEqual(seals[0]["kind"], "GENESIS")
        self.assertEqual(seals[0]["row_count"], 2)

        store.persist_transaction(5001, "0/2000", "2026-11-18 10:00:00+00",
                                  [change(1003), change(1002, "UPDATE", version="2.1")])
        store.persist_transaction(5002, "0/3000", None, [change(1001, "DELETE")])
        seals = store._load_seals_locked()
        self.assertEqual([s["seq"] for s in seals], [0, 1, 2])
        # chain_hash 严格衔接
        self.assertEqual(
            seals[1]["chain_hash"],
            seal.compute_chain_hash(seals[0]["chain_hash"], seals[1]["entry_hash"]),
        )
        # 全链与各截止点核验均通过
        latest = lsn_to_num("0/3000")
        r = store.verify_at(latest)
        self.assertEqual(r["status"], seal.VERIFIED)
        self.assertEqual(r["checked_seals"], 3)
        self.assertFalse(r["seal_genesis"]["legacy_upgrade"])
        mid = store.verify_at(lsn_to_num("0/2000"))
        self.assertEqual(mid["status"], seal.VERIFIED)
        self.assertEqual(mid["checked_seals"], 2)
        # 回溯行全部可信（新装库基线即封存起点，启用后未变即可信）
        self.assertTrue(all(a["trust"] == seal.TRUSTED for a in r["artifacts"]))
        # 1001 已删除，不在目录；历史链仍在且带 trust
        hist = store.get_history_of(1001)
        self.assertEqual([h["change_type"] for h in hist], ["BASELINE", "DELETE"])
        self.assertTrue(all(h["trust"] == seal.TRUSTED for h in hist))
        store.close()

    def test_replay_idempotent_no_reseal(self):
        store = self._new_store()
        self._baseline_and_stream(store)
        self.assertTrue(store.persist_transaction(6001, "0/2000", None, [change(1009)]))
        # 同一 xid 重放（强杀重启后槽位重放）-> 跳过，不重复封存
        self.assertFalse(store.persist_transaction(6001, "0/2000", None, [change(1009)]))
        self.assertEqual([s["seq"] for s in store._load_seals_locked()], [0, 1])
        self.assertEqual(store.get_progress()["captured_transactions"], 1)
        store.close()

    def test_reopen_chain_intact_and_continue(self):
        store = self._new_store()
        self._baseline_and_stream(store)
        store.persist_transaction(7001, "0/2000", None, [change(1010)])
        store.close()
        # 等价于强杀后重启：用同库文件重建 store
        store2 = self._new_store()
        self.assertEqual([s["seq"] for s in store2._load_seals_locked()], [0, 1])
        self.assertEqual(store2.verify_at(lsn_to_num("0/2000"))["status"], seal.VERIFIED)
        # 续接新事务，链从 seq=2 继续，无空洞
        store2.persist_transaction(7002, "0/3000", None, [change(1011)])
        self.assertEqual([s["seq"] for s in store2._load_seals_locked()], [0, 1, 2])
        self.assertEqual(store2.verify_at(lsn_to_num("0/3000"))["status"], seal.VERIFIED)
        store2.close()

    def test_legacy_upgrade_marks_old_history_unknown(self):
        # 先手工构造一个“旧版本”库：无 seals 表、artifact_history 无 seal_seq
        conn = sqlite3.connect(self.path)
        conn.executescript(
            f"""
            CREATE TABLE capture_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO capture_state VALUES('phase','STREAMING');
            INSERT INTO capture_state VALUES('baseline_lsn','0/1000');
            CREATE TABLE txns(xid INTEGER PRIMARY KEY, commit_lsn TEXT NOT NULL,
                              commit_lsn_num INTEGER NOT NULL, commit_ts TEXT);
            CREATE TABLE artifact_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT, artifact_no INTEGER NOT NULL,
                change_type TEXT NOT NULL, service_name TEXT, version TEXT,
                sha256 TEXT, xid INTEGER, commit_lsn TEXT,
                commit_lsn_num INTEGER, captured_at TEXT NOT NULL);
            INSERT INTO artifact_history(artifact_no,change_type,service_name,version,
                sha256,xid,commit_lsn,commit_lsn_num,captured_at)
            VALUES (1001,'BASELINE','old','1','{SHA}',NULL,'0/1000',4096,'t0'),
                   (1001,'UPDATE','new','2','{SHA}',9001,'0/2000',8192,'t1');
            INSERT INTO txns VALUES(9001,'0/2000',8192,NULL);
            """
        )
        conn.commit()
        conn.close()

        store = self._new_store()  # 构造即触发首次启用封存
        seals = store._load_seals_locked()
        self.assertEqual(len(seals), 1)
        self.assertEqual(seals[0]["kind"], "GENESIS")
        self.assertEqual(seals[0]["row_count"], 2)
        self.assertTrue(store.chain_quick_status()["legacy_upgrade"])
        # 旧历史已整体封存，链可复算通过 -> VERIFIED，但这些行是“启用前”记录，
        # trust 由行级语义标为 UNKNOWN（启用前真实性未知）
        r = store.verify_at(lsn_to_num("0/2000"))
        self.assertEqual(r["status"], seal.VERIFIED)
        self.assertTrue(r["seal_genesis"]["legacy_upgrade"])
        hist = store.get_history_of(1001)
        self.assertTrue(all(h["trust"] == seal.UNKNOWN for h in hist))
        # 启用之后的新事务被正常链式封存，且这些新行可信
        store.persist_transaction(9002, "0/3000", None, [change(1002)])
        r2 = store.verify_at(lsn_to_num("0/3000"))
        self.assertEqual(r2["status"], seal.VERIFIED)
        by_no = {a["artifact_no"]: a["trust"] for a in r2["artifacts"]}
        self.assertEqual(by_no[1002], seal.TRUSTED)
        self.assertEqual(by_no[1001], seal.UNKNOWN)
        store.close()

    # ------------------------------------------------------- 篡改检测
    def _seed_three_seals(self):
        store = self._new_store()
        self._baseline_and_stream(store)
        store.persist_transaction(8001, "0/2000", None, [change(1003)])
        store.persist_transaction(8002, "0/3000", None, [change(1004)])
        store.persist_transaction(8003, "0/4000", None, [change(1005)])
        return store

    def test_detect_value_tamper_points_first_invalid(self):
        store = self._seed_three_seals()
        # 直接改 SQLite 里 seq=2 对应行的值（模拟误改旧提交点记录）
        with store._conn:
            store._conn.execute(
                "UPDATE artifact_history SET version='HACKED' WHERE seal_seq=2"
            )
        r = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r["status"], seal.INVALID)
        self.assertEqual(r["first_invalid_seq"], 2)
        self.assertEqual(r["first_invalid_reason"], seal.REASON_ENTRY_HASH)
        # seq<2 的行仍可信；seq>=2 不可信
        trusts = {a["artifact_no"]: a["trust"] for a in r["artifacts"]}
        self.assertEqual(trusts[1001], seal.TRUSTED)
        self.assertEqual(trusts[1004], seal.UNTRUSTED)
        self.assertEqual(trusts[1005], seal.UNTRUSTED)
        store.close()

    def test_detect_missing_row(self):
        store = self._seed_three_seals()
        with store._conn:
            store._conn.execute("DELETE FROM artifact_history WHERE seal_seq=1")
        r = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r["status"], seal.INVALID)
        self.assertEqual(r["first_invalid_seq"], 1)
        self.assertEqual(r["first_invalid_reason"], seal.REASON_ROW_COUNT)
        store.close()

    def test_detect_deleted_seal_seq_gap(self):
        store = self._seed_three_seals()
        # 删除整笔封存 seq=2：链出现空洞。audit 逐行扫描，seq=3 先暴露空洞，
        # 但其上链式摘要也同时断裂——最早可定位的失效位置即空洞之后第一笔。
        with store._conn:
            store._conn.execute("DELETE FROM seals WHERE seq=2")
        r = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r["first_invalid_reason"], seal.REASON_SEQ_GAP)
        self.assertEqual(r["first_invalid_seq"], 3)
        self.assertEqual(r["last_good_seq"], 1)
        store.close()

    def test_detect_chain_break_on_reseal(self):
        store = self._seed_three_seals()
        # 连同历史行一起“重封”seq=1：entry_hash 能对上当前行（改值不报警），
        # 但新 entry_hash 与真实前驱的链式衔接必然断裂 -> CHAIN_BROKEN
        with store._conn:
            store._conn.execute(
                "UPDATE artifact_history SET version='HACKED' WHERE seal_seq=1"
            )
            rows = store._rows_for_seal(1, lsn_to_num("0/2000"))
            s1 = next(s for s in store._load_seals_locked() if s["seq"] == 1)
            new_entry = seal.compute_entry_hash(
                seal.KIND_TXN, 1, s1["xid"], s1["commit_lsn_num"], rows
            )
            store._conn.execute(
                "UPDATE seals SET entry_hash=? WHERE seq=1", (new_entry,)
            )
        r = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r["status"], seal.INVALID)
        self.assertEqual(r["first_invalid_seq"], 1)
        self.assertEqual(r["first_invalid_reason"], seal.REASON_CHAIN)
        self.assertFalse(store.chain_quick_status()["link_ok"])
        store.close()

    def test_detect_out_of_order(self):
        store = self._seed_three_seals()
        with store._conn:
            # 把 seq=2 的提交点改到比 seq=1 更早 -> 乱序
            store._conn.execute(
                "UPDATE seals SET commit_lsn_num=? WHERE seq=2",
                (lsn_to_num("0/1500"),),
            )
        r = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r["first_invalid_reason"], seal.REASON_ORDER)
        self.assertEqual(r["first_invalid_seq"], 2)
        store.close()

    def test_verify_at_earlier_point_isolates_later_tamper(self):
        store = self._seed_three_seals()
        with store._conn:
            store._conn.execute(
                "UPDATE artifact_history SET version='HACKED' WHERE seal_seq=3"
            )
        # 截止 seq=2 的提交点核验：篡改发生在其后，核验范围不含 seq=3，仍通过
        r = store.verify_at(lsn_to_num("0/3000"))
        self.assertEqual(r["status"], seal.VERIFIED)
        self.assertEqual(r["checked_seals"], 3)  # genesis + txn1 + txn2
        # 到最新点则失败
        r2 = store.verify_at(lsn_to_num("0/4000"))
        self.assertEqual(r2["status"], seal.INVALID)
        self.assertEqual(r2["first_invalid_seq"], 3)
        store.close()

    def test_persist_failure_leaves_no_partial_seal_or_lsn(self):
        store = self._new_store()
        self._baseline_and_stream(store)
        before = store.get_progress()["last_commit_lsn"]
        # 人为让封存写入失败：seals 表已存在，再建同名触发器不现实，
        # 改为传入不可序列化数据触发同事务异常，断言整体回滚。
        bad = [{"artifact_no": 1, "change_type": "INSERT",
                "service_name": object(), "version": "1", "sha256": SHA}]
        with self.assertRaises(Exception):
            store.persist_transaction(8101, "0/2000", None, bad)
        # 事务、数据、封存、确认位置都没有被推进
        self.assertEqual(store.get_progress()["captured_transactions"], 0)
        self.assertEqual([s["seq"] for s in store._load_seals_locked()], [0])
        self.assertEqual(store.get_progress()["last_commit_lsn"], before)
        # 同一 xid 之后可正常重试落盘（无半条残留）
        self.assertTrue(
            store.persist_transaction(8101, "0/2000", None, [change(1009)])
        )
        self.assertEqual(store.verify_at(lsn_to_num("0/2000"))["status"], seal.VERIFIED)
        store.close()


if __name__ == "__main__":
    unittest.main()
