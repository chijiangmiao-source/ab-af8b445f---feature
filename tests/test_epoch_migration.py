"""纪元迁移状态机的单元测试。"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import ApiError, Store  # noqa: E402


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db_path, page_ttl_seconds=45)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------ 辅助

    def make_ws(self, name="样地A", record_count=0):
        ws = self.store.create_workspace(name)
        page = self.store.open_page(ws["id"])
        for i in range(record_count):
            self.store.add_record(ws["id"], page["page_id"], f"观测记录-{i + 1}")
        return ws, page["page_id"]

    def drive_to(self, ws_id, page_id, target_phase, version="v2", batch=1, start=True):
        """把迁移推进到指定阶段。start=False 表示迁移已发起，直接续推。"""
        if start:
            self.store.start_migration(ws_id, page_id, version)
        if target_phase == "copying":
            return
        while True:
            s = self.store.copy_batch(ws_id, page_id, batch)
            if s["migration"]["phase"] == "validating":
                break
        if target_phase == "validating":
            return
        self.store.validate_migration(ws_id, page_id)
        if target_phase == "publishing":
            return
        self.store.publish_migration(ws_id, page_id)

    def assert_api_error(self, status, code, fn, *args, **kwargs):
        with self.assertRaises(ApiError) as ctx:
            fn(*args, **kwargs)
        self.assertEqual(ctx.exception.status, status)
        self.assertEqual(ctx.exception.code, code)
        return ctx.exception

    # ------------------------------------------------------------ 基础

    def test_workspace_starts_at_epoch_one(self):
        ws, _ = self.make_ws()
        self.assertEqual(ws["current_epoch"]["number"], 1)
        self.assertEqual(ws["current_epoch"]["version"], "v1")
        self.assertEqual(ws["records"], [])

    def test_add_and_read_records(self):
        ws, page = self.make_ws(record_count=3)
        s = self.store.get_state(ws["id"])
        self.assertEqual([r["content"] for r in s["records"]],
                         ["观测记录-1", "观测记录-2", "观测记录-3"])
        self.assertEqual([r["seq"] for r in s["records"]], [1, 2, 3])

    def test_write_requires_active_page(self):
        ws, page = self.make_ws()
        self.store.close_page(ws["id"], page)
        self.assert_api_error(409, "page_not_active",
                              self.store.add_record, ws["id"], page, "迟到记录")

    # ------------------------------------------------------------ 迁移主流程

    def test_full_migration_and_stale_write_rejected(self):
        """两个页面打开同一工作区：迁移完成后，另一页的迟到保存被拒绝。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]

        self.store.start_migration(ws["id"], page_a, "v2")
        # 发布前：迁移进行中，另一页的保存被拒绝
        self.assert_api_error(409, "migration_in_progress",
                              self.store.add_record, ws["id"], page_b, "迟到记录")
        # 复制 -> 校验 -> 发布
        self.drive_to(ws["id"], page_a, "publishing", start=False)
        self.store.publish_migration(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(s["current_epoch"]["version"], "v2")
        self.assertEqual([r["content"] for r in s["records"]],
                         [f"观测记录-{i}" for i in range(1, 5)])
        # 两个旧页面均已失效
        states = {p["id"]: p["state"] for p in s["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        # 发布后：旧页面的迟到保存仍被拒绝并提示重新载入
        err = self.assert_api_error(409, "page_not_active",
                                    self.store.add_record, ws["id"], page_b, "迟到记录")
        self.assertIn("重新载入", err.message)
        # 重开页面后只能读到新纪元，且可继续写入
        page_c = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_c, "新纪元记录")
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["records"][-1]["content"], "新纪元记录")

    def test_concurrent_migration_creates_no_second_candidate(self):
        ws, page_a = self.make_ws(record_count=2)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.start_migration(ws["id"], page_a, "v2")
        self.assert_api_error(409, "migration_active",
                              self.store.start_migration, ws["id"], page_b, "v2b")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)

    def test_reads_never_come_from_candidate(self):
        """复制进行中读取到的仍是完整旧纪元，候选的部分数据不可见。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)  # 只复制 2/5
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["migration"]["copied"], 2)

    # ------------------------------------------------------------ 中断恢复

    def test_copy_interruption_recycles_candidate(self):
        """复制阶段页面关闭：候选被安全回收，绝不展示部分复制数据。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)
        self.store.close_page(ws["id"], page_a)  # 模拟页面在复制之间关闭

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertIsNone(s["migration"]["candidate_epoch"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)  # 完整旧纪元
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 0)

    def test_validating_resumes_same_candidate(self):
        """校验阶段页面关闭：后来页面续用同一候选并完成迁移。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand_before = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "validating")
        self.assertEqual(s["migration"]["candidate_epoch"], cand_before)  # 同一候选

        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.validate_migration(ws["id"], page_b)
        self.store.publish_migration(ws["id"], page_b)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_publishing_completes_after_owner_close(self):
        """发布阶段页面关闭：恢复时把原子发布补齐。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "publishing")
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "published")
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_crash_recovery_on_reopen_store(self):
        """模拟进程在复制中途崩溃：重开存储后候选被回收，数据不残缺。"""
        ws, page_a = self.make_ws(record_count=4)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 1)
        self.store.close()  # 模拟进程崩溃（事务已提交到 copying 阶段）

        self.store = Store(self.db_path, page_ttl_seconds=45)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 4)

    # ------------------------------------------------------------ 校验与持久化

    def test_validate_mismatch_marks_failed_and_allows_retry(self):
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        # 人为破坏候选内容
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        s = self.store.validate_migration(ws["id"], page_a)
        self.assertEqual(s["migration"]["phase"], "failed")
        # 失败后可重新发起：旧候选被回收，新候选唯一
        self.store.start_migration(ws["id"], page_a, "v2")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)
        self.drive_to(ws["id"], page_a, "published", start=False)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_persistence_after_publish(self):
        """发布后重开存储：纪元、记录、页面失效状态一致。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.drive_to(ws["id"], page_a, "published")
        before = self.store.get_state(ws["id"])
        self.store.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        after = self.store.get_state(ws["id"])
        self.assertEqual(after["current_epoch"], before["current_epoch"])
        self.assertEqual([r["content"] for r in after["records"]],
                         [r["content"] for r in before["records"]])
        states = {p["id"]: p["state"] for p in after["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        self.assertEqual(after["migration"]["phase"], "published")

    # ------------------------------------------------------------ 谱系

    def test_lineage_origin_epoch_marked_no_source(self):
        """旧版起始纪元：明确标示为无迁移来源，每条记录均为原始记录。"""
        ws, page = self.make_ws(record_count=2)
        ep_id = ws["current_epoch"]["id"]
        lin = self.store.get_lineage(ws["id"], ep_id)
        self.assertIsNone(lin["source_epoch"])
        self.assertIsNone(lin["lineage_frozen_at"])
        self.assertFalse(lin["epoch"]["has_migration_source"])
        self.assertEqual([e["origin"] for e in lin["entries"]], ["origin", "origin"])
        self.assertTrue(all("无迁移来源" in e["origin_label"] for e in lin["entries"]))
        self.assertTrue(all(e["source_seq"] is None for e in lin["entries"]))
        # 每条记录带稳定摘要
        self.assertTrue(all(e["target_digest"] for e in lin["entries"]))

    def test_lineage_frozen_positionally_with_duplicate_contents(self):
        """映射按记录出现顺序逐位固化；正文相同的独立记录分别保留。"""
        ws = self.store.create_workspace("重复正文样地")
        page = self.store.open_page(ws["id"])["page_id"]
        for content in ("重复观测", "普通观测", "重复观测"):
            self.store.add_record(ws["id"], page, content)
        self.drive_to(ws["id"], page, "published")
        s = self.store.get_state(ws["id"])
        new_ep = s["current_epoch"]["id"]

        lin = self.store.get_lineage(ws["id"], new_ep)
        self.assertTrue(lin["epoch"]["has_migration_source"])
        self.assertEqual(lin["source_epoch"]["number"], 1)
        self.assertEqual(lin["lineage_count"], 3)
        self.assertIsNotNone(lin["lineage_frozen_at"])
        origins = [(e["target_seq"], e["source_seq"], e["origin"]) for e in lin["entries"]]
        # 按出现顺序：#1↔#1、#2↔#2、#3↔#3，三条都迁移而来，重复正文不合并
        self.assertEqual(origins, [(1, 1, "migrated"), (2, 2, "migrated"),
                                   (3, 3, "migrated")])
        # 两条“重复观测”是不同记录，但两侧稳定摘要相同且可对拍
        first, third = lin["entries"][0], lin["entries"][2]
        self.assertNotEqual(first["target_record_id"], third["target_record_id"])
        self.assertEqual(first["target_digest"], first["source_digest"])
        self.assertEqual(third["target_digest"], third["source_digest"])
        self.assertEqual(first["target_digest"], third["target_digest"])
        self.assertTrue(all("迁移而来" in e["origin_label"] for e in lin["entries"]))

    def test_lineage_marks_records_created_after_publish(self):
        """发布后在本纪元新建的记录在谱系中标示为本纪元新建，其余为迁移而来。"""
        ws, page_a = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page_a, "published")
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_b, "新纪元新增")
        self.store.add_record(ws["id"], page_b, "新纪元新增二")

        ep_id = self.store.get_state(ws["id"])["current_epoch"]["id"]
        lin = self.store.get_lineage(ws["id"], ep_id)
        self.assertEqual(
            [(e["target_seq"], e["origin"], e["source_seq"]) for e in lin["entries"]],
            [(1, "migrated", 1), (2, "migrated", 2),
             (3, "created", None), (4, "created", None)],
        )

    def test_lineage_chains_across_multiple_migrations(self):
        ws, page = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page, "published", version="v2")
        page = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page, "纪元2记录")
        self.drive_to(ws["id"], page, "published", version="v3")
        s = self.store.get_state(ws["id"])
        ep3 = s["current_epoch"]["id"]

        epochs = {e["number"]: e for e in self.store.list_epochs(ws["id"])["epochs"]}
        self.assertEqual(set(epochs), {1, 2, 3})
        self.assertEqual(epochs[3]["source_epoch"]["number"], 2)
        self.assertTrue(epochs[2]["has_migration_source"])
        self.assertFalse(epochs[1]["has_migration_source"])

        lin = self.store.get_lineage(ws["id"], ep3)
        self.assertEqual(lin["source_epoch"]["number"], 2)
        # 纪元2新建的那条迁移到纪元3时，源序号是它在纪元2的序号 #3
        self.assertEqual(
            [(e["target_seq"], e["source_seq"]) for e in lin["entries"]],
            [(1, 1), (2, 2), (3, 3)],
        )

    def test_lineage_unavailable_for_candidate_and_recycled_epochs(self):
        """候选（复制中/校验中/失败）与已回收纪元不得暴露任何部分映射。"""
        ws, page = self.make_ws(record_count=3)
        self.store.start_migration(ws["id"], page, "v2")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        self.store.copy_batch(ws["id"], page, 1)  # 只复制 1/3
        self.assert_api_error(404, "epoch_lineage_unavailable",
                              self.store.get_lineage, ws["id"], cand)
        # 列表同样不暴露候选
        listed = {e["id"] for e in self.store.list_epochs(ws["id"])["epochs"]}
        self.assertNotIn(cand, listed)
        # 复制中断 -> 候选回收，仍不可见
        self.store.close_page(ws["id"], page)
        self.assert_api_error(404, "epoch_lineage_unavailable",
                              self.store.get_lineage, ws["id"], cand)

    def test_lineage_unavailable_for_failed_candidate(self):
        ws, page = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page, "validating")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        self.store.validate_migration(ws["id"], page)
        self.assert_api_error(404, "epoch_lineage_unavailable",
                              self.store.get_lineage, ws["id"], cand)

    def test_lineage_mapping_is_immutable(self):
        """固化映射不可修改、不可删除（SQLite 触发器兜底）。"""
        import sqlite3
        ws, page = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page, "published")
        ep = self.store.get_state(ws["id"])["current_epoch"]["id"]
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.conn.execute(
                "UPDATE record_lineage SET source_seq=99 WHERE target_epoch_id=?", (ep,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.conn.execute(
                "DELETE FROM record_lineage WHERE target_epoch_id=?", (ep,))

    def test_lineage_persists_across_restart(self):
        """重开存储后，历史纪元与其谱系保持一致。"""
        ws, page = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page, "published")
        s = self.store.get_state(ws["id"])
        ep2 = s["current_epoch"]["id"]
        ep1 = self.store.get_lineage(ws["id"], ep2)["source_epoch"]["id"]
        before = self.store.get_lineage(ws["id"], ep2)
        self.store.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        after = self.store.get_lineage(ws["id"], ep2)
        self.assertEqual(
            [(e["target_seq"], e["source_seq"]) for e in after["entries"]],
            [(e["target_seq"], e["source_seq"]) for e in before["entries"]],
        )
        self.assertEqual(after["lineage_digest"], before["lineage_digest"])
        # 起始纪元仍标示无迁移来源
        origin = self.store.get_lineage(ws["id"], ep1)
        self.assertFalse(origin["epoch"]["has_migration_source"])
        self.assertTrue(all(e["origin"] == "origin" for e in origin["entries"]))
        # 历史纪元与当前纪元都在列表中
        numbers = [e["number"] for e in self.store.list_epochs(ws["id"])["epochs"]]
        self.assertEqual(numbers, [1, 2])

    def test_lineage_backfilled_for_legacy_database(self):
        """旧版库升级：起始纪元无来源，旧发布纪元按位置回填，重复正文逐位保留。"""
        import sqlite3
        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.executescript(
            "CREATE TABLE workspaces (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
            "created_at TEXT NOT NULL, current_epoch_id TEXT, migration_phase TEXT NOT NULL "
            "DEFAULT 'idle', migration_target_version TEXT, migration_candidate_epoch_id TEXT, "
            "migration_source_epoch_id TEXT, migration_owner_page_id TEXT, "
            "migration_copied INTEGER NOT NULL DEFAULT 0, migration_total INTEGER NOT NULL "
            "DEFAULT 0, migration_started_at TEXT, migration_updated_at TEXT, migration_error TEXT);"
            "CREATE TABLE epochs (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, "
            "number INTEGER NOT NULL, version TEXT NOT NULL, kind TEXT NOT NULL, "
            "created_at TEXT NOT NULL, UNIQUE (workspace_id, number));"
            "CREATE TABLE records (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, "
            "epoch_id TEXT NOT NULL, seq INTEGER NOT NULL, content TEXT NOT NULL, "
            "created_at TEXT NOT NULL);"
            "CREATE TABLE pages (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, "
            "epoch_id TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, "
            "last_seen TEXT NOT NULL, closed_at TEXT, invalidated_at TEXT);"
        )
        conn.execute("INSERT INTO workspaces(id,name,created_at,current_epoch_id) "
                     "VALUES('ws1','旧站','t','ep2')")
        conn.execute("INSERT INTO epochs VALUES('ep1','ws1',1,'v1','superseded','t')")
        conn.execute("INSERT INTO epochs VALUES('ep2','ws1',2,'v2','published','t')")
        rows = [("ep1", 1, "重复观测"), ("ep1", 2, "别的观测"), ("ep1", 3, "重复观测"),
                ("ep2", 1, "重复观测"), ("ep2", 2, "别的观测"), ("ep2", 3, "重复观测"),
                ("ep2", 4, "新纪元记录")]
        for i, (eid, seq, txt) in enumerate(rows):
            conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                         (f"r{i}", "ws1", eid, seq, txt, "t"))
        conn.commit()
        conn.close()

        store = Store(legacy, page_ttl_seconds=45)
        lin = store.get_lineage("ws1", "ep2")
        self.assertEqual(
            [(e["target_seq"], e["source_seq"], e["origin"]) for e in lin["entries"]],
            [(1, 1, "migrated"), (2, 2, "migrated"), (3, 3, "migrated"),
             (4, None, "created")],
        )
        origin = store.get_lineage("ws1", "ep1")
        self.assertFalse(origin["epoch"]["has_migration_source"])
        self.assertTrue(all(e["origin"] == "origin" for e in origin["entries"]))
        store.close()


if __name__ == "__main__":
    unittest.main()
