"""SQLite 历史库：只增不改的变更日志 + 捕获状态 + 连续封存哈希链。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更、该事务的提交点与其封存摘要在同一个 SQLite
  事务里落盘，保证多行事务整体可见、不会出现半条事务或“数据已写、封存缺失”；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据、更不会重复封存；
- 封存表 seals 只追加：GENESIS（封存起点，仅证明启用后未变）+
  每笔 TXN（entry_hash 覆盖提交点与全部变更，chain_hash 衔接前驱）。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from . import seal

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    captured_at  TEXT NOT NULL,
    seal_seq     INTEGER                     -- 所属封存序号；NULL=早于封存启用
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);

-- 连续封存：只追加，不允许 UPDATE/DELETE（应用层也从不修改）
CREATE TABLE IF NOT EXISTS seals (
    seq            INTEGER PRIMARY KEY,      -- 0=GENESIS，其后每笔源事务 +1
    kind           TEXT NOT NULL,            -- GENESIS / TXN
    xid            INTEGER,
    commit_lsn     TEXT,
    commit_lsn_num INTEGER NOT NULL,
    entry_hash     TEXT NOT NULL,            -- 覆盖提交点 + 全部变更行
    chain_hash     TEXT NOT NULL,            -- SHA256(前驱 chain_hash || entry_hash)
    row_count      INTEGER NOT NULL,
    sealed_at      TEXT NOT NULL
);
"""

# 依赖 seal_seq 列的索引必须在旧库 ALTER TABLE 迁移之后再创建
SEAL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_hist_seal ON artifact_history(seal_seq);
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)
        self._migrate_legacy_seal_schema()
        self._conn.executescript(SEAL_INDEX_SQL)
        self._enable_sealing_on_legacy_history()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- migration
    def _column_names(self, table: str) -> set[str]:
        return {
            r["name"]
            for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }

    def _migrate_legacy_seal_schema(self) -> None:
        """旧版本库（无 seal_seq / seals 表）平滑升级，已有数据一行不丢。"""
        with self._lock:
            cols = self._column_names("artifact_history")
            if "seal_seq" not in cols:
                self._conn.execute(
                    "ALTER TABLE artifact_history ADD COLUMN seal_seq INTEGER"
                )

    def _enable_sealing_on_legacy_history(self) -> None:
        """首次启用封存：对启用时刻已存在的全部历史补一条 GENESIS 封存。

        仅证明“此后未变”；这些旧行 seal_seq=0 但核验时按封存起点处理，
        其启用前真实性标为 UNKNOWN（由核验侧 trust_label 区分 NULL 旧行与
        新装库的基线行——新装库基线同样在 genesis 内，语义一致：起点本身
        不可由链自证，只在链完好时保证启用后未被改动）。
        """
        with self._lock:
            seal_rows = self._conn.execute("SELECT COUNT(*) AS c FROM seals").fetchone()
            if seal_rows["c"] > 0:
                return
            hist_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            if hist_count == 0:
                return  # 全新库：genesis 随 save_baseline 一起落盘
            state = self.get_state()
            lsn = state.get("baseline_lsn") or "0/0"
            lsn_num = lsn_to_num(lsn)
            rows = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT artifact_no, change_type, service_name, version, sha256"
                    " FROM artifact_history ORDER BY id"
                ).fetchall()
            ]
            now = utc_now_iso()
            entry_hash = seal.compute_entry_hash(
                seal.KIND_GENESIS, seal.GENESIS_SEQ, None, lsn_num, rows
            )
            chain_hash = seal.compute_chain_hash(None, entry_hash)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE artifact_history SET seal_seq=? WHERE seal_seq IS NULL",
                    (seal.GENESIS_SEQ,),
                )
                self._conn.execute(
                    "INSERT INTO seals(seq, kind, xid, commit_lsn, commit_lsn_num,"
                    " entry_hash, chain_hash, row_count, sealed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        seal.GENESIS_SEQ, seal.KIND_GENESIS, None, lsn, lsn_num,
                        entry_hash, chain_hash, len(rows), now,
                    ),
                )
                self._set_many(
                    {
                        "sealing_enabled_at": now,
                        "seal_genesis_lsn": lsn,
                        "seal_genesis_legacy": 1,
                    }
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        return self.get_phase() == PHASE_STREAMING

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线、基线提交点与 GENESIS 封存起点在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        now,
                        seal.GENESIS_SEQ,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at, seal_seq)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                # GENESIS：封存启用时刻的整张基线目录，仅证明此后未变
                seal_rows = [
                    {
                        "artifact_no": r[0],
                        "change_type": "BASELINE",
                        "service_name": r[1],
                        "version": r[2],
                        "sha256": r[3],
                    }
                    for r in rows
                ]
                entry_hash = seal.compute_entry_hash(
                    seal.KIND_GENESIS, seal.GENESIS_SEQ, None, lsn_num, seal_rows
                )
                chain_hash = seal.compute_chain_hash(None, entry_hash)
                self._conn.execute(
                    "INSERT INTO seals(seq, kind, xid, commit_lsn, commit_lsn_num,"
                    " entry_hash, chain_hash, row_count, sealed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        seal.GENESIS_SEQ, seal.KIND_GENESIS, None, lsn, lsn_num,
                        entry_hash, chain_hash, len(seal_rows), now,
                    ),
                )
                self._set_many(
                    {
                        "sealing_enabled_at": now,
                        "seal_genesis_lsn": lsn,
                        "seal_genesis_legacy": 0,
                    }
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> bool:
        """落盘一个已提交源事务及其链式封存。

        数据行、提交点、封存摘要在同一个 SQLite 事务提交：任一失败整体回滚，
        捕获线程因此不会向源端确认该事务（调用方仅在返回成功后 send_feedback）。
        返回 True=新写入，False=重复事务（重放，幂等跳过，不会重复封存）。
        """
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                next_seq = (
                    self._conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM seals"
                    ).fetchone()["s"]
                )
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
                    " VALUES (?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at, seal_seq)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            now,
                            next_seq,
                        )
                        for c in changes
                    ],
                )
                # 链式封存：entry_hash 覆盖提交点与该事务全部变更（有序）
                entry_hash = seal.compute_entry_hash(
                    seal.KIND_TXN, next_seq, xid, lsn_num,
                    [
                        {
                            "artifact_no": c["artifact_no"],
                            "change_type": c["change_type"],
                            "service_name": c.get("service_name"),
                            "version": c.get("version"),
                            "sha256": c.get("sha256"),
                        }
                        for c in changes
                    ],
                )
                prev_chain = self._conn.execute(
                    "SELECT chain_hash FROM seals WHERE seq=(SELECT MAX(seq) FROM seals)"
                ).fetchone()["chain_hash"]
                chain_hash = seal.compute_chain_hash(prev_chain, entry_hash)
                self._conn.execute(
                    "INSERT INTO seals(seq, kind, xid, commit_lsn, commit_lsn_num,"
                    " entry_hash, chain_hash, row_count, sealed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        next_seq, seal.KIND_TXN, xid, commit_lsn, lsn_num,
                        entry_hash, chain_hash, len(changes), now,
                    ),
                )
                # 槽位推进位置与数据、封存在同一事务落盘
                self._set_many(
                    {
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                        "last_seal_seq": next_seq,
                    }
                )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                        "seal_seq": seal.GENESIS_SEQ,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count,
                       (SELECT s.seq FROM seals s WHERE s.xid = t.xid) AS seal_seq
                FROM txns t ORDER BY t.commit_lsn_num, t.xid
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                        "seal_seq": row["seal_seq"],
                    }
                )
            return points

    def get_directory_at(self, lsn_num: int) -> list[dict[str, Any]]:
        """查看某个提交点（含）为止的整目录：每个编号取最后一条变更。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        """单编号完整变更链，并按全链核验结果给每行打可信标记。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at, seal_seq
                FROM artifact_history WHERE artifact_no=?
                ORDER BY id
                """,
                (artifact_no,),
            ).fetchall()
            audit = self._audit_locked(None)
            # 旧库首次启用封存：封存起点覆盖的旧行，启用前真实性未知
            legacy = self.get_state().get("seal_genesis_legacy") == "1"
            result = []
            for r in rows:
                item = dict(r)
                item["trust"] = self._trust_for(
                    r["seal_seq"], audit["first_invalid_seq"], legacy
                )
                result.append(item)
            return result

    @staticmethod
    def _trust_for(
        seal_seq: int | None, first_invalid_seq: int | None, legacy_genesis: bool
    ) -> str:
        if seal_seq is None:
            return seal.UNKNOWN
        if first_invalid_seq is not None and seal_seq >= first_invalid_seq:
            return seal.UNTRUSTED
        if legacy_genesis and seal_seq == seal.GENESIS_SEQ:
            return seal.UNKNOWN  # 启用封存前已存在的旧记录，真实性未知
        return seal.TRUSTED

    # ------------------------------------------------------------- 封存核验
    def _load_seals_locked(self) -> list[dict[str, Any]]:
        return [
            dict(r)
            for r in self._conn.execute(
                "SELECT seq, kind, xid, commit_lsn, commit_lsn_num, entry_hash,"
                " chain_hash, row_count, sealed_at FROM seals ORDER BY seq"
            ).fetchall()
        ]

    def _rows_for_seal(self, seq: int, bound_lsn_num: int) -> list[dict[str, Any]]:
        with self._lock:
            if seq == seal.GENESIS_SEQ:
                # GENESIS 整体封存启用时刻的全部历史，不按提交点截断
                rows = self._conn.execute(
                    """
                    SELECT artifact_no, change_type, service_name, version, sha256
                    FROM artifact_history
                    WHERE seal_seq=?
                    ORDER BY id
                    """,
                    (seq,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT artifact_no, change_type, service_name, version, sha256
                    FROM artifact_history
                    WHERE seal_seq=? AND commit_lsn_num <= ?
                    ORDER BY id
                    """,
                    (seq, bound_lsn_num),
                ).fetchall()
            return [dict(r) for r in rows]

    def _audit_locked(self, point_lsn_num: int | None) -> dict[str, Any]:
        seals = self._load_seals_locked()
        return seal.audit_chain(
            seals,
            lambda s, bound: self._rows_for_seal(s, bound),
            point_lsn_num,
        )

    def audit(self, point_lsn_num: int | None = None) -> dict[str, Any]:
        """全链（或截止某提交点）核验：复算 entry_hash/chain_hash、查缺行乱序。"""
        with self._lock:
            return self._audit_locked(point_lsn_num)

    def verify_at(self, lsn_num: int) -> dict[str, Any]:
        """以同一历史视图核验截止该提交点的记录，并给回溯结果打可信标记。"""
        with self._lock:
            audit = self._audit_locked(lsn_num)
            first_invalid = audit["first_invalid_seq"]
            state = self.get_state()
            rows = self._conn.execute(
                """
                SELECT h.artifact_no, h.service_name, h.version, h.sha256,
                       h.change_type, h.seal_seq, h.commit_lsn_num, h.xid
                FROM artifact_history h
                WHERE h.id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND h.change_type != 'DELETE'
                ORDER BY h.artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            artifacts = []
            legacy = state.get("seal_genesis_legacy") == "1"
            for r in rows:
                item = dict(r)
                item["trust"] = self._trust_for(
                    r["seal_seq"], first_invalid, legacy
                )
                artifacts.append(item)

            genesis = self._conn.execute(
                "SELECT * FROM seals WHERE seq=?", (seal.GENESIS_SEQ,)
            ).fetchone()
            return {
                "point_lsn_num": lsn_num,
                "status": audit["status"],
                "first_invalid_seq": first_invalid,
                "first_invalid_reason": audit["first_invalid_reason"],
                "first_invalid_detail": audit["first_invalid_detail"],
                "last_good_seq": audit["last_good_seq"],
                "checked_seals": audit["checked_seals"],
                "total_seals": self._conn.execute(
                    "SELECT COUNT(*) AS c FROM seals"
                ).fetchone()["c"],
                "seal_genesis": (
                    {
                        "seq": genesis["seq"],
                        "commit_lsn": genesis["commit_lsn"],
                        "sealed_at": genesis["sealed_at"],
                        "legacy_upgrade": state.get("seal_genesis_legacy") == "1",
                    }
                    if genesis
                    else None
                ),
                "artifacts": artifacts,
            }

    def chain_quick_status(self) -> dict[str, Any]:
        """轻量链状态（不重算业务行哈希，供进度卡片高频刷新）。

        完整的改值/缺行核验走 /api/verify；这里只检查封存序号连续性与
        chain_hash 衔接，能立刻发现封存表被删/被替换/乱序。
        """
        with self._lock:
            seals = self._load_seals_locked()
            if not seals:
                return {"enabled": False, "seal_count": 0, "link_ok": None}
            prev_chain: str | None = None
            expected = seal.GENESIS_SEQ
            link_ok = True
            for s in seals:
                if int(s["seq"]) != expected:
                    link_ok = False
                    break
                if seal.compute_chain_hash(prev_chain, s["entry_hash"]) != s["chain_hash"]:
                    link_ok = False
                    break
                prev_chain = s["chain_hash"]
                expected += 1
            state = self.get_state()
            return {
                "enabled": True,
                "seal_count": len(seals),
                "link_ok": link_ok,
                "genesis_lsn": state.get("seal_genesis_lsn"),
                "sealing_enabled_at": state.get("sealing_enabled_at"),
                "legacy_upgrade": state.get("seal_genesis_legacy") == "1",
            }

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
                "seal": self.chain_quick_status(),
            }
