"""连续封存：链式 SHA-256 摘要与网页核验的纯逻辑（无 IO，便于单测）。

封存模型
--------
- 首次启用时落一条 **GENESIS（封存起点）** 记录，对启用时刻已经存在的
  全部历史做一次整体摘要。它只能证明“启用封存之后这些行未被改动”，
  启用之前的真实性无法由链证明，核验时这些行标记为 UNKNOWN（未知）。
- 此后每个源事务落盘时，在**同一个 SQLite 事务**里追加一条 TXN 封存：
  封存内容为该提交点（LSN/xid）+ 该事务的全部变更行，entry_hash 覆盖
  这些内容，chain_hash = SHA256(上一条 chain_hash || entry_hash)，形成
  只能追加、无法暗中改写的哈希链。

核验能发现的三类篡改（对应需求：缺行、改值、乱序）
----------------------------------------------------
1. 改值：重算的 entry_hash 与封存值不一致（ENTRY_HASH_MISMATCH）；
2. 缺行：实际行数与封存 row_count 不一致（ROW_COUNT_MISMATCH），
   或链中存在 seq 空洞（SEQ_GAP，删掉整笔封存时）；
3. 乱序：commit_lsn_num 不随 seq 单调递增（ORDER_MISMATCH），
   或 chain_hash 衔接断裂（CHAIN_BROKEN）。
核验一旦发现问题，返回**最早失效的封存（first_invalid_seq）**；其之前的
封存仍可独立复算通过，之后（含）的回溯结果一律标记 UNTRUSTED。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Sequence

# 核验总体状态
VERIFIED = "VERIFIED"    # 核验范围内每条封存都可独立复算通过
INVALID = "INVALID"      # 链被破坏（缺行/改值/乱序/断链），存在不可信结果
UNKNOWN = "UNKNOWN"      # 数据早于封存起点，真实性未知（不代表被篡改）

# 单行回溯结果的可信标记
TRUSTED = "TRUSTED"
UNTRUSTED = "UNTRUSTED"

# 封存类型
KIND_GENESIS = "GENESIS"
KIND_TXN = "TXN"

# 失效原因码
REASON_ENTRY_HASH = "ENTRY_HASH_MISMATCH"
REASON_CHAIN = "CHAIN_BROKEN"
REASON_ROW_COUNT = "ROW_COUNT_MISMATCH"
REASON_SEQ_GAP = "SEQ_GAP"
REASON_ORDER = "ORDER_MISMATCH"

GENESIS_SEQ = 0


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj: Any) -> bytes:
    """确定性 JSON：固定分隔符、保留 UTF-8、键序按插入顺序（调用方保证）。"""
    return json.dumps(
        obj, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    ).encode("utf-8")


def canonical_row(row: dict[str, Any]) -> dict[str, Any]:
    """单行变更的规范化形态：固定键序与类型，任何一列被改都会改变摘要。"""
    return {
        "artifact_no": int(row["artifact_no"]),
        "change_type": str(row["change_type"]),
        "service_name": (
            None if row.get("service_name") is None else str(row["service_name"])
        ),
        "version": None if row.get("version") is None else str(row["version"]),
        "sha256": None if row.get("sha256") is None else str(row["sha256"]),
    }


def entry_header(kind: str, seq: int, xid: int | None, lsn_num: int) -> dict[str, Any]:
    return {
        "kind": kind,
        "seq": int(seq),
        "xid": None if xid is None else int(xid),
        "commit_lsn_num": int(lsn_num),
    }


def compute_entry_hash(
    kind: str,
    seq: int,
    xid: int | None,
    lsn_num: int,
    rows: Iterable[dict[str, Any]],
) -> str:
    """对“封存头 + 全部变更行（有序）”做 SHA-256。"""
    payload = {
        "header": entry_header(kind, seq, xid, lsn_num),
        "rows": [canonical_row(r) for r in rows],
    }
    return sha256_hex(canonical_json(payload))


def compute_chain_hash(prev_chain_hash: str | None, entry_hash: str) -> str:
    """链式摘要：genesis 的前驱为空串，之后每条衔接上一条 chain_hash。"""
    prev = prev_chain_hash or ""
    return sha256_hex(prev.encode("ascii") + bytes.fromhex(entry_hash))


def trust_label(
    seal_seq: int | None, first_invalid_seq: int | None
) -> str:
    """根据链核验结果给单行回溯记录打可信标记。

    - 早于封存起点（seal_seq 为 NULL）：真实性未知（UNKNOWN）；
    - 链完好：启用后封存覆盖的行可信（TRUSTED）；
    - 链在某点失效：失效点及其之后的行不可信（UNTRUSTED），
      失效点之前已独立验证通过的行仍可信。
    """
    if seal_seq is None:
        return UNKNOWN
    if first_invalid_seq is not None and seal_seq >= first_invalid_seq:
        return UNTRUSTED
    return TRUSTED


def audit_chain(
    seals: Sequence[dict[str, Any]],
    rows_for_seq,
    point_lsn_num: int | None = None,
) -> dict[str, Any]:
    """逐条复算哈希链并核验。纯函数，所有数据由调用方从 SQLite 取出。

    seals: 全链封存（按 seq 升序），每项含
           seq/kind/xid/commit_lsn_num/entry_hash/chain_hash/row_count。
    rows_for_seq(seq, lsn_num): 返回该封存覆盖、且 commit_lsn_num <= lsn_num
           （point 核验时截断）的 artifact_history 行（按 id 升序）。
    point_lsn_num: 仅核验截止该提交点的封存；None 表示全链。

    返回:
      {status, first_invalid_seq, first_invalid_reason, first_invalid_detail,
       last_good_seq, checked_seals, genesis_seq, point_lsn_num}
    只记录最早失效点；失效点之后不再给出可信结论。
    """
    first_invalid_seq: int | None = None
    first_invalid_reason: str | None = None
    first_invalid_detail: str | None = None
    last_good_seq: int | None = None
    prev_chain: str | None = None
    prev_lsn_num: int | None = None
    expected_seq = GENESIS_SEQ
    checked = 0

    for seal in seals:
        seq = int(seal["seq"])
        if point_lsn_num is not None and int(seal["commit_lsn_num"]) > point_lsn_num:
            break  # 截止该点：之后的封存不在本次核验范围
        checked += 1

        def _fail(reason: str, detail: str) -> None:
            nonlocal first_invalid_seq, first_invalid_reason, first_invalid_detail
            if first_invalid_seq is None:
                first_invalid_seq = seq
                first_invalid_reason = reason
                first_invalid_detail = detail

        # 1) 乱序/缺整笔封存：seq 必须从 0 起严格连续
        if seq != expected_seq:
            _fail(REASON_SEQ_GAP, f"封存序列不连续：期望 seq={expected_seq}，实际 seq={seq}")
        # 2) 乱序：提交点必须随 seq 单调递增
        if (
            prev_lsn_num is not None
            and int(seal["commit_lsn_num"]) < prev_lsn_num
        ):
            _fail(
                REASON_ORDER,
                f"提交点乱序：seq={seq} 的 LSN {seal['commit_lsn_num']}"
                f" 早于上一封存 {prev_lsn_num}",
            )

        # 3) 用当前实际历史行复算 entry_hash（改值/缺行都会暴露）。
        #    GENESIS 是对启用时刻整张历史的整体快照，必须按其自身封存时刻
        #    整体复算，不按查询点截断（否则旧库升级时晚于基线 LSN 的旧行会
        #    被误判为缺行）；其后的 TXN 封存才按 point 截断。
        if int(seal["seq"]) == GENESIS_SEQ:
            bound = int(seal["commit_lsn_num"])
        else:
            bound = (
                point_lsn_num
                if point_lsn_num is not None
                else int(seal["commit_lsn_num"])
            )
        actual_rows = rows_for_seq(seq, bound) or []
        recomputed_entry = compute_entry_hash(
            str(seal["kind"]),
            seq,
            seal["xid"],
            int(seal["commit_lsn_num"]),
            actual_rows,
        )
        if recomputed_entry != str(seal["entry_hash"]):
            # 进一步区分：行数对不上 => 缺行；否则视为改值
            if len(actual_rows) != int(seal["row_count"]):
                _fail(
                    REASON_ROW_COUNT,
                    f"seq={seq} 封存 {seal['row_count']} 行，实际 {len(actual_rows)} 行（缺行/多行）",
                )
            else:
                _fail(
                    REASON_ENTRY_HASH,
                    f"seq={seq} 复算摘要与封存不一致（数据被改值）",
                )
        elif len(actual_rows) != int(seal["row_count"]):
            _fail(
                REASON_ROW_COUNT,
                f"seq={seq} 封存 {seal['row_count']} 行，实际 {len(actual_rows)} 行（缺行/多行）",
            )

        # 4) 链式 SHA-256 衔接
        recomputed_chain = compute_chain_hash(prev_chain, str(seal["entry_hash"]))
        if recomputed_chain != str(seal["chain_hash"]):
            _fail(
                REASON_CHAIN,
                f"seq={seq} 链式摘要衔接断裂（封存被替换/重封）",
            )

        if first_invalid_seq is None:
            last_good_seq = seq
        prev_chain = str(seal["chain_hash"])
        prev_lsn_num = int(seal["commit_lsn_num"])
        expected_seq = seq + 1

    status = INVALID if first_invalid_seq is not None else VERIFIED
    return {
        "status": status,
        "first_invalid_seq": first_invalid_seq,
        "first_invalid_reason": first_invalid_reason,
        "first_invalid_detail": first_invalid_detail,
        "last_good_seq": last_good_seq,
        "checked_seals": checked,
        "genesis_seq": GENESIS_SEQ,
        "point_lsn_num": point_lsn_num,
    }
