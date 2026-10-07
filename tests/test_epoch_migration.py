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

    def _epoch_id(self, ws_id, number):
        return self.store.conn.execute(
            "SELECT id FROM epochs WHERE workspace_id=? AND number=?",
            (ws_id, number)).fetchone()["id"]

    def test_origin_epoch_marked_without_migration_source(self):
        """旧版起始纪元：无迁移来源，记录逐条标示为本纪元创建。"""
        ws, page = self.make_ws(record_count=2)
        ep1 = self._epoch_id(ws["id"], 1)
        lin = self.store.get_lineage(ws["id"], ep1)
        self.assertIsNone(lin["origin"])
        self.assertEqual([e["origin"] for e in lin["entries"]], ["created", "created"])
        self.assertTrue(all(e["source_seq"] is None for e in lin["entries"]))
        self.assertTrue(all(e["target_digest"] for e in lin["entries"]))

    def test_lineage_built_in_order_with_duplicate_content(self):
        """映射按记录出现顺序逐行固化；正文重复的独立记录分别保留。"""
        ws = self.store.create_workspace("重复正文样地")
        page = self.store.open_page(ws["id"])["page_id"]
        for text in ("降雨", "降雨", "降雨", "物候A"):
            self.store.add_record(ws["id"], page, text)
        self.drive_to(ws["id"], page, "published")

        ep2 = self._epoch_id(ws["id"], 2)
        lin = self.store.get_lineage(ws["id"], ep2)
        self.assertIsNotNone(lin["origin"])
        self.assertEqual(lin["origin"]["source_number"], 1)
        # 四条记录（含三条同文）各自成行，目标/源序号严格按出现位置一一对应
        self.assertEqual([(e["target_seq"], e["source_seq"], e["origin"])
                          for e in lin["entries"]],
                         [(1, 1, "migrated"), (2, 2, "migrated"),
                          (3, 3, "migrated"), (4, 4, "migrated")])
        self.assertTrue(all(e["target_digest"] == e["source_digest"]
                            for e in lin["entries"]))
        # 持久化表中同样是四行独立映射，没有按正文合并
        rows = self.store.conn.execute(
            "SELECT target_seq, source_seq FROM lineage_mappings "
            "WHERE target_epoch_id=? ORDER BY target_seq", (ep2,)).fetchall()
        self.assertEqual([(r["target_seq"], r["source_seq"]) for r in rows],
                         [(1, 1), (2, 2), (3, 3), (4, 4)])

    def test_records_created_after_migration_marked_created(self):
        """迁移后在新纪元新建的记录：无源位置，标示为本纪元创建。"""
        ws, page_a = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page_a, "published")
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_b, "迁移后新观测")
        ep2 = self._epoch_id(ws["id"], 2)
        lin = self.store.get_lineage(ws["id"], ep2)
        self.assertEqual([(e["target_seq"], e["source_seq"], e["origin"])
                          for e in lin["entries"]],
                         [(1, 1, "migrated"), (2, 2, "migrated"),
                          (3, None, "created")])

    def test_chain_migration_lineage_points_at_immediate_source(self):
        """连续迁移：第二跳的来源是紧邻的上一个纪元，新建记录映射到其实际位置。"""
        ws, page_a = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page_a, "published", version="v2")
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_b, "新纪元独有")
        self.drive_to(ws["id"], page_b, "published", version="v3")
        ep3 = self._epoch_id(ws["id"], 3)
        lin = self.store.get_lineage(ws["id"], ep3)
        self.assertEqual(lin["origin"]["source_number"], 2)
        self.assertEqual([(e["target_seq"], e["source_seq"], e["origin"])
                          for e in lin["entries"]],
                         [(1, 1, "migrated"), (2, 2, "migrated"),
                          (3, 3, "migrated")])

    def test_candidate_and_recycled_epochs_expose_no_lineage(self):
        """复制中/校验中/失败的候选与回收候选：一律不暴露部分映射。"""
        ws, page_a = self.make_ws(record_count=3)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        # 复制进行中：候选不可读谱系，且映射表里没有任何片段
        self.assert_api_error(409, "epoch_not_published",
                              self.store.get_lineage, ws["id"], cand)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM lineage_mappings").fetchone()["c"], 0)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epoch_origins").fetchone()["c"], 0)
        # 中断回收：候选纪元消失
        self.store.close_page(ws["id"], page_a)
        self.assert_api_error(404, "epoch_not_found",
                              self.store.get_lineage, ws["id"], cand)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM lineage_mappings").fetchone()["c"], 0)

    def test_failed_candidate_exposes_no_lineage(self):
        """校验失败的候选仍未发布：谱系不可用；重试成功后映射才固化。"""
        ws, page_a = self.make_ws(record_count=2)
        self.drive_to(ws["id"], page_a, "validating")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        self.store.validate_migration(ws["id"], page_a)
        self.assert_api_error(409, "epoch_not_published",
                              self.store.get_lineage, ws["id"], cand)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.drive_to(ws["id"], page_a, "published", start=False)
        ep2 = self._epoch_id(ws["id"], 2)
        lin = self.store.get_lineage(ws["id"], ep2)
        self.assertEqual(len(lin["entries"]), 2)
        self.assertTrue(all(e["origin"] == "migrated" for e in lin["entries"]))

    def test_only_published_epochs_listed(self):
        """纪元选择只含已发布纪元，候选从不出现。"""
        ws, page_a = self.make_ws(record_count=2)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 1)
        numbers = [e["number"] for e in self.store.list_published_epochs(ws["id"])]
        self.assertEqual(numbers, [1])
        self.drive_to(ws["id"], page_a, "published", start=False)
        epochs = self.store.list_published_epochs(ws["id"])
        self.assertEqual([(e["number"], e["kind"]) for e in epochs],
                         [(1, "superseded"), (2, "published")])

    def test_lineage_persists_across_restart(self):
        """重开存储后历史纪元谱系与来源凭据保持一致。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "published")
        ep2 = self._epoch_id(ws["id"], 2)
        before = self.store.get_lineage(ws["id"], ep2)
        self.store.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        after = self.store.get_lineage(ws["id"], ep2)
        self.assertEqual(after["origin"], before["origin"])
        self.assertEqual([(e["target_seq"], e["source_seq"], e["origin"],
                           e["target_digest"], e["source_digest"])
                          for e in after["entries"]],
                         [(e["target_seq"], e["source_seq"], e["origin"],
                           e["target_digest"], e["source_digest"])
                          for e in before["entries"]])
        # 历史起始纪元仍明确标示无迁移来源
        ep1 = self._epoch_id(ws["id"], 1)
        self.assertIsNone(self.store.get_lineage(ws["id"], ep1)["origin"])


if __name__ == "__main__":
    unittest.main()
