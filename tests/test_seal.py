"""封存链纯逻辑单测：python3 -m unittest tests.test_seal -v"""
import unittest

from app import seal


def txn_rows(*nos):
    return [
        {
            "artifact_no": no,
            "change_type": "INSERT",
            "service_name": f"svc-{no}",
            "version": "1.0.0",
            "sha256": "a" * 64,
        }
        for no in nos
    ]


class SealHashTest(unittest.TestCase):
    def test_entry_hash_deterministic_and_sensitive(self):
        rows = txn_rows(1, 2)
        h1 = seal.compute_entry_hash(seal.KIND_TXN, 1, 100, 0x1000, rows)
        h2 = seal.compute_entry_hash(seal.KIND_TXN, 1, 100, 0x1000, [dict(r) for r in rows])
        self.assertEqual(h1, h2)  # 确定性
        # 改任意一列 -> 摘要变化
        changed = [dict(r) for r in rows]
        changed[0]["version"] = "9.9.9"
        self.assertNotEqual(
            h1, seal.compute_entry_hash(seal.KIND_TXN, 1, 100, 0x1000, changed)
        )
        # 缺一行 -> 摘要变化
        self.assertNotEqual(
            h1, seal.compute_entry_hash(seal.KIND_TXN, 1, 100, 0x1000, rows[:1])
        )
        # 乱序（交换行顺序）-> 摘要变化
        swapped = [rows[1], rows[0]]
        self.assertNotEqual(
            h1, seal.compute_entry_hash(seal.KIND_TXN, 1, 100, 0x1000, swapped)
        )

    def test_chain_hash_links(self):
        e0 = seal.compute_entry_hash(seal.KIND_GENESIS, 0, None, 0x100, txn_rows(1))
        c0 = seal.compute_chain_hash(None, e0)
        e1 = seal.compute_entry_hash(seal.KIND_TXN, 1, 10, 0x200, txn_rows(2))
        c1 = seal.compute_chain_hash(c0, e1)
        self.assertNotEqual(c0, c1)
        # 换一个 entry -> 链摘要变化
        e1b = seal.compute_entry_hash(seal.KIND_TXN, 1, 10, 0x200, txn_rows(3))
        self.assertNotEqual(c1, seal.compute_chain_hash(c0, e1b))
        # 改前驱 -> 链摘要变化
        self.assertNotEqual(c1, seal.compute_chain_hash("f" * 64, e1))

    def test_audit_clean_chain_verified(self):
        seals = []
        rows_by_seq = {}
        prev = None
        e = seal.compute_entry_hash(seal.KIND_GENESIS, 0, None, 0x100, txn_rows(1))
        c = seal.compute_chain_hash(prev, e)
        seals.append(dict(seq=0, kind="GENESIS", xid=None, commit_lsn_num=0x100,
                          entry_hash=e, chain_hash=c, row_count=1))
        rows_by_seq[0] = txn_rows(1)
        prev = c
        for i, lsn in enumerate((0x200, 0x300), start=1):
            rows = txn_rows(10 + i)
            e = seal.compute_entry_hash(seal.KIND_TXN, i, 100 + i, lsn, rows)
            c = seal.compute_chain_hash(prev, e)
            seals.append(dict(seq=i, kind="TXN", xid=100 + i, commit_lsn_num=lsn,
                              entry_hash=e, chain_hash=c, row_count=len(rows)))
            rows_by_seq[i] = rows
            prev = c
        audit = seal.audit_chain(seals, lambda s, bound: rows_by_seq[s])
        self.assertEqual(audit["status"], seal.VERIFIED)
        self.assertIsNone(audit["first_invalid_seq"])
        self.assertEqual(audit["checked_seals"], 3)
        # 截止中间点只核验前两条
        mid = seal.audit_chain(seals, lambda s, bound: rows_by_seq[s], 0x200)
        self.assertEqual(mid["checked_seals"], 2)
        self.assertEqual(mid["status"], seal.VERIFIED)

    def test_audit_detects_value_tamper(self):
        seals, rows_by_seq, prev = [], {}, None
        for seq, lsn in ((0, 0x100), (1, 0x200), (2, 0x300)):
            kind = "GENESIS" if seq == 0 else "TXN"
            rows = txn_rows(seq + 1)
            e = seal.compute_entry_hash(kind, seq, None if seq == 0 else 100 + seq, lsn, rows)
            c = seal.compute_chain_hash(prev, e)
            seals.append(dict(seq=seq, kind=kind,
                              xid=None if seq == 0 else 100 + seq,
                              commit_lsn_num=lsn, entry_hash=e, chain_hash=c,
                              row_count=len(rows)))
            rows_by_seq[seq] = [dict(r) for r in rows]
            prev = c
        # 篡改 seq=1 的值
        rows_by_seq[1][0]["sha256"] = "b" * 64
        audit = seal.audit_chain(seals, lambda s, bound: rows_by_seq[s])
        self.assertEqual(audit["status"], seal.INVALID)
        self.assertEqual(audit["first_invalid_seq"], 1)
        self.assertEqual(audit["first_invalid_reason"], seal.REASON_ENTRY_HASH)
        self.assertEqual(audit["last_good_seq"], 0)

    def test_audit_detects_missing_row_and_seq_gap_and_order(self):
        seals, rows_by_seq, prev = [], {}, None
        for seq, lsn in ((0, 0x100), (1, 0x200), (2, 0x300)):
            rows = txn_rows(seq + 1, seq + 2)
            e = seal.compute_entry_hash(
                "GENESIS" if seq == 0 else "TXN", seq,
                None if seq == 0 else 100 + seq, lsn, rows)
            c = seal.compute_chain_hash(prev, e)
            seals.append(dict(seq=seq, kind="GENESIS" if seq == 0 else "TXN",
                              xid=None if seq == 0 else 100 + seq,
                              commit_lsn_num=lsn, entry_hash=e, chain_hash=c,
                              row_count=len(rows)))
            rows_by_seq[seq] = [dict(r) for r in rows]
            prev = c
        # 缺行：seq=2 实际少一行（row_count=2 但只给 1 行）
        rows_by_seq[2] = rows_by_seq[2][:1]
        audit = seal.audit_chain(seals, lambda s, bound: rows_by_seq[s])
        self.assertEqual(audit["first_invalid_reason"], seal.REASON_ROW_COUNT)
        self.assertEqual(audit["first_invalid_seq"], 2)

        # seq 空洞（删掉整笔封存 seq=1）
        gapped = [seals[0], seals[2]]
        rb = {0: rows_by_seq[0], 2: txn_rows(3, 4)}
        audit = seal.audit_chain(gapped, lambda s, bound: rb.get(s, []))
        self.assertEqual(audit["first_invalid_reason"], seal.REASON_SEQ_GAP)

        # 乱序：seq=1 的 LSN 早于 seq=0
        reordered = [dict(s) for s in seals]
        reordered[1]["commit_lsn_num"] = 0x50
        rb2 = {0: txn_rows(1, 2), 1: txn_rows(2, 3), 2: txn_rows(3, 4)}
        audit = seal.audit_chain(reordered, lambda s, bound: rb2[s])
        self.assertEqual(audit["first_invalid_reason"], seal.REASON_ORDER)

    def test_trust_labels(self):
        self.assertEqual(seal.trust_label(None, None), seal.UNKNOWN)
        self.assertEqual(seal.trust_label(0, None), seal.TRUSTED)
        self.assertEqual(seal.trust_label(5, None), seal.TRUSTED)
        # 失效点及其之后不可信，之前仍可信
        self.assertEqual(seal.trust_label(2, 2), seal.UNTRUSTED)
        self.assertEqual(seal.trust_label(3, 2), seal.UNTRUSTED)
        self.assertEqual(seal.trust_label(1, 2), seal.TRUSTED)


if __name__ == "__main__":
    unittest.main()
