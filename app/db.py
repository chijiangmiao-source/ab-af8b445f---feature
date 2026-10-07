"""纪元（epoch）存储层：工作区、记录、页面围栏与迁移状态机。

设计约束（对应野外实验站离线记录页的升级要求）：

- 读取永远只来自工作区指针指向的「已发布」纪元；候选纪元从不对外提供读取，
  因此读结果只能是完整旧纪元或完整新纪元。
- 迁移状态存放在工作区行上（唯一槽位），并发迁移不可能创建第二个候选纪元。
- 迁移协议：复制到候选纪元 -> 校验 -> 单事务原子发布（切换指针 + 作废旧纪元
  + 失效旧页面一次提交）。发布前后，旧页面的迟到保存一律被拒绝并提示重新载入。
- 页面在复制/校验/发布之间关闭时，依据持久化阶段恢复：
  复制中 -> 安全回收候选；校验中 -> 保留同一候选等待续用；发布中 -> 立即完成发布。
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

# 迁移阶段：idle -> copying -> validating -> publishing -> published
# 异常终态：failed（校验失败）、aborted（复制中断后候选被回收）
ACTIVE_PHASES = ("copying", "validating", "publishing")
TERMINAL_PHASES = ("idle", "published", "failed", "aborted")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  current_epoch_id TEXT,
  -- 迁移唯一槽位：同一工作区至多一个进行中的迁移，天然杜绝第二个候选纪元
  migration_phase TEXT NOT NULL DEFAULT 'idle',
  migration_target_version TEXT,
  migration_candidate_epoch_id TEXT,
  migration_source_epoch_id TEXT,
  migration_owner_page_id TEXT,
  migration_copied INTEGER NOT NULL DEFAULT 0,
  migration_total INTEGER NOT NULL DEFAULT 0,
  migration_started_at TEXT,
  migration_updated_at TEXT,
  migration_error TEXT
);
CREATE TABLE IF NOT EXISTS epochs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  number INTEGER NOT NULL,
  version TEXT NOT NULL,
  kind TEXT NOT NULL,              -- published | candidate | superseded
  created_at TEXT NOT NULL,
  -- 谱系凭据：迁移来源纪元与发布事务内固化的谱系清单；NULL 表示起始纪元（无迁移来源）
  source_epoch_id TEXT,
  lineage_frozen_at TEXT,
  lineage_count INTEGER NOT NULL DEFAULT 0,
  lineage_digest TEXT,
  UNIQUE (workspace_id, number)
);
CREATE TABLE IF NOT EXISTS records (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  epoch_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_epoch ON records(epoch_id, seq);
-- 不可变谱系映射：目标纪元每条记录（按出现顺序）指向源纪元记录；
-- 仅在发布事务内随指针切换一并固化。触发器禁止任何事后修改/删除，
-- 重复正文也按位置分别成行，不会因内容相同而合并。
CREATE TABLE IF NOT EXISTS record_lineage (
  target_epoch_id TEXT NOT NULL,
  target_seq INTEGER NOT NULL,
  target_record_id TEXT NOT NULL,
  source_epoch_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  source_record_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (target_epoch_id, target_seq)
);
CREATE INDEX IF NOT EXISTS idx_lineage_source ON record_lineage(source_epoch_id, source_seq);
CREATE TRIGGER IF NOT EXISTS trg_lineage_no_update
BEFORE UPDATE ON record_lineage
BEGIN
  SELECT RAISE(ABORT, 'record_lineage 为不可变映射，禁止修改');
END;
CREATE TRIGGER IF NOT EXISTS trg_lineage_no_delete
BEFORE DELETE ON record_lineage
BEGIN
  SELECT RAISE(ABORT, 'record_lineage 为不可变映射，禁止删除');
END;
CREATE TABLE IF NOT EXISTS pages (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  epoch_id TEXT NOT NULL,          -- 页面持有的围栏：打开时锁定的纪元
  state TEXT NOT NULL,             -- active | invalidated | closed
  created_at TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  closed_at TEXT,
  invalidated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_pages_ws ON pages(workspace_id);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class ApiError(Exception):
    """携带 HTTP 状态码与业务错误码的异常，extra 会并入错误响应体。"""

    def __init__(self, status: int, code: str, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra or {}


class Store:
    def __init__(self, path: str, page_ttl_seconds: int = 45):
        self.path = path
        self.page_ttl = timedelta(seconds=page_ttl_seconds)
        self._lock = threading.RLock()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._migrate_schema_locked()
        # 进程重启 = 所有页面连接已断开：关闭遗留活动页面并恢复未完成的迁移
        self.recover_all()

    def _migrate_schema_locked(self):
        """旧库升级：补齐谱系列并回填既有已发布纪元的谱系。

        起始纪元（首个已发布纪元）明确标记为无迁移来源；更早版本发布的纪元虽无
        历史映射行，但旧迁移协议按 seq 复制并通过摘要校验，可据现存记录按位置
        重建映射，发布后新建而多出的序号自然留空（谱系中标示为本纪元新建）。
        """
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(epochs)").fetchall()}
        upgraded = "source_epoch_id" not in cols
        if "source_epoch_id" not in cols:
            self.conn.execute("ALTER TABLE epochs ADD COLUMN source_epoch_id TEXT")
        if "lineage_frozen_at" not in cols:
            self.conn.execute("ALTER TABLE epochs ADD COLUMN lineage_frozen_at TEXT")
        if "lineage_count" not in cols:
            self.conn.execute("ALTER TABLE epochs ADD COLUMN lineage_count INTEGER NOT NULL DEFAULT 0")
        if "lineage_digest" not in cols:
            self.conn.execute("ALTER TABLE epochs ADD COLUMN lineage_digest TEXT")
        if upgraded:
            with self._tx():
                self._backfill_legacy_lineage_locked()

    def _backfill_legacy_lineage_locked(self):
        """为旧版已发布纪元按纪元序号链与记录 seq 重建谱系（幂等）。"""
        workspaces = self.conn.execute("SELECT id FROM workspaces").fetchall()
        for w in workspaces:
            epochs = self.conn.execute(
                "SELECT id, number, created_at FROM epochs "
                "WHERE workspace_id=? AND kind IN ('published','superseded') ORDER BY number",
                (w["id"],),
            ).fetchall()
            prev = None
            for ep in epochs:
                if prev is None:
                    # 首个已发布纪元即起始纪元：显式无迁移来源
                    self.conn.execute(
                        "UPDATE epochs SET source_epoch_id=NULL, lineage_frozen_at=NULL, "
                        "lineage_count=0, lineage_digest=NULL WHERE id=?",
                        (ep["id"],),
                    )
                elif self.conn.execute(
                    "SELECT COUNT(*) AS c FROM record_lineage WHERE target_epoch_id=?",
                    (ep["id"],),
                ).fetchone()["c"] == 0:
                    self._backfill_epoch_lineage_locked(prev, ep)
                prev = ep

    def _backfill_epoch_lineage_locked(self, src: sqlite3.Row, dst: sqlite3.Row):
        """按共同 seq 回填旧纪元 dst 的迁移映射（重复正文同样逐位成行）。"""
        src_rows = {r["seq"]: r for r in self.conn.execute(
            "SELECT id, seq, content FROM records WHERE epoch_id=?", (src["id"],)).fetchall()}
        h = hashlib.sha256()
        count = 0
        for d in self.conn.execute(
            "SELECT id, seq, content FROM records WHERE epoch_id=? ORDER BY seq",
            (dst["id"],),
        ).fetchall():
            s = src_rows.get(d["seq"])
            if s is None or s["content"] != d["content"]:
                continue  # 源侧缺失或内容不符：视为本纪元新建，谱系中如实留空
            self.conn.execute(
                "INSERT OR IGNORE INTO record_lineage(target_epoch_id,target_seq,"
                "target_record_id,source_epoch_id,source_seq,source_record_id,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (dst["id"], d["seq"], d["id"], src["id"], s["seq"], s["id"], dst["created_at"]),
            )
            h.update(f"{d['seq']}={s['seq']}\n".encode())
            count += 1
        self.conn.execute(
            "UPDATE epochs SET source_epoch_id=?, lineage_frozen_at=?, lineage_count=?, "
            "lineage_digest=? WHERE id=?",
            (src["id"], dst["created_at"], count, h.hexdigest(), dst["id"]),
        )

    def close(self):
        with self._lock:
            self.conn.close()

    # ---------------------------------------------------------------- 基础工具

    @contextmanager
    def _tx(self):
        """BEGIN IMMEDIATE：立即取得写锁，事务内的 检查-再写入 不会被并发穿插。"""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def _ws(self, ws_id: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        if row is None:
            raise ApiError(404, "workspace_not_found", "工作区不存在")
        return row

    def _page(self, page_id: str | None) -> sqlite3.Row | None:
        if not page_id:
            return None
        return self.conn.execute("SELECT * FROM pages WHERE id=?", (page_id,)).fetchone()

    def _epoch(self, epoch_id: str | None) -> sqlite3.Row | None:
        if not epoch_id:
            return None
        return self.conn.execute("SELECT * FROM epochs WHERE id=?", (epoch_id,)).fetchone()

    def _checksum(self, epoch_id: str) -> tuple[int, str]:
        """纪元的 (记录数, 内容摘要)，用于复制后校验与发布前复核。"""
        rows = self.conn.execute(
            "SELECT seq, content FROM records WHERE epoch_id=? ORDER BY seq", (epoch_id,)
        ).fetchall()
        h = hashlib.sha256()
        for r in rows:
            h.update(f"{r['seq']}|".encode())
            h.update(r["content"].encode())
            h.update(b"\n")
        return (len(rows), h.hexdigest())

    @staticmethod
    def _record_digest(content: str) -> str:
        """单条记录的稳定摘要：同正文必得同摘要，便于复核员对拍两侧记录。"""
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def _require_driver(self, ws: sqlite3.Row, page: sqlite3.Row | None):
        """迁移操作只能由持有当前纪元围栏的活动页面发起。"""
        if page is None or page["workspace_id"] != ws["id"]:
            raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
        if page["state"] != "active":
            raise ApiError(409, "page_not_active", "本页面已失效或已关闭，请重新载入",
                           {"page_state": page["state"]})
        if page["epoch_id"] != ws["current_epoch_id"]:
            raise ApiError(409, "stale_epoch", "页面围栏已过期，请重新载入")

    # ---------------------------------------------------------- 维护与崩溃恢复

    def _maintenance_locked(self, ws_id: str):
        """惰性维护：过期心跳页面置为已关闭；属主页面失联的迁移按持久化阶段恢复。"""
        with self._tx():
            cutoff = (datetime.now(timezone.utc) - self.page_ttl).isoformat(timespec="milliseconds")
            self.conn.execute(
                "UPDATE pages SET state='closed', closed_at=? "
                "WHERE workspace_id=? AND state='active' AND last_seen < ?",
                (utcnow(), ws_id, cutoff),
            )
            ws = self._ws(ws_id)
            if ws["migration_phase"] in ACTIVE_PHASES:
                owner = self._page(ws["migration_owner_page_id"])
                if owner is None or owner["state"] != "active":
                    self._recover_locked(ws)

    def _recover_locked(self, ws: sqlite3.Row):
        """依据持久化阶段恢复：复制中->回收候选；校验中->续用同一候选；发布中->完成发布。"""
        phase = ws["migration_phase"]
        cand = ws["migration_candidate_epoch_id"]
        now = utcnow()
        if phase == "copying":
            # 复制可能只完成了一部分：安全回收候选，绝不展示部分复制的数据
            if cand:
                self.conn.execute("DELETE FROM record_lineage WHERE target_epoch_id=?", (cand,))
                self.conn.execute("DELETE FROM records WHERE epoch_id=?", (cand,))
                self.conn.execute("DELETE FROM epochs WHERE id=?", (cand,))
            self.conn.execute(
                "UPDATE workspaces SET migration_phase='aborted', "
                "migration_candidate_epoch_id=NULL, migration_source_epoch_id=NULL, "
                "migration_owner_page_id=NULL, migration_copied=0, migration_total=0, "
                "migration_error=?, migration_updated_at=? WHERE id=?",
                ("负责迁移的页面在复制阶段关闭，候选纪元已安全回收", now, ws["id"]),
            )
        elif phase == "validating":
            # 复制已完成：保留同一候选纪元，等待任一活动页面继续校验/发布
            self.conn.execute(
                "UPDATE workspaces SET migration_owner_page_id=NULL, migration_updated_at=? WHERE id=?",
                (now, ws["id"]),
            )
        elif phase == "publishing":
            # 发布事务未提交即中断：候选已校验，恢复时把发布补齐（幂等）
            self._publish_locked(ws)

    def recover_all(self):
        """进程启动时调用：上一生命周期的页面全部视为已关闭，并恢复所有迁移。"""
        with self._lock:
            with self._tx():
                self.conn.execute(
                    "UPDATE pages SET state='closed', closed_at=? WHERE state='active'",
                    (utcnow(),),
                )
                rows = self.conn.execute(
                    "SELECT * FROM workspaces WHERE migration_phase IN ('copying','validating','publishing')"
                ).fetchall()
                for ws in rows:
                    self._recover_locked(ws)

    # ---------------------------------------------------------------- 工作区

    def create_workspace(self, name: str) -> dict:
        name = (name or "").strip()
        if not name:
            raise ApiError(400, "bad_name", "工作区名称不能为空")
        if len(name) > 80:
            raise ApiError(400, "bad_name", "工作区名称过长")
        with self._lock:
            with self._tx():
                ws_id = new_id("ws")
                epoch_id = new_id("ep")
                now = utcnow()
                try:
                    self.conn.execute(
                        "INSERT INTO workspaces(id,name,created_at,current_epoch_id,migration_phase) "
                        "VALUES(?,?,?,?,'idle')",
                        (ws_id, name, now, epoch_id),
                    )
                except sqlite3.IntegrityError:
                    raise ApiError(409, "name_taken", "同名工作区已存在")
                self.conn.execute(
                    "INSERT INTO epochs(id,workspace_id,number,version,kind,created_at) "
                    "VALUES(?,?,?,?,'published',?)",
                    (epoch_id, ws_id, 1, "v1", now),
                )
        return self.get_state(ws_id)

    def list_workspaces(self) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT w.id, w.name, w.created_at, w.migration_phase, "
                "       e.number AS epoch_number, e.version AS epoch_version "
                "FROM workspaces w LEFT JOIN epochs e ON e.id = w.current_epoch_id "
                "ORDER BY w.created_at"
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------------------------------------------------------------- 页面

    def open_page(self, ws_id: str) -> dict:
        """打开页面：在当前已发布纪元上建立围栏。触发一次惰性恢复。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                page_id = new_id("pg")
                now = utcnow()
                self.conn.execute(
                    "INSERT INTO pages(id,workspace_id,epoch_id,state,created_at,last_seen) "
                    "VALUES(?,?,?,'active',?,?)",
                    (page_id, ws_id, ws["current_epoch_id"], now, now),
                )
                epoch = self._epoch(ws["current_epoch_id"])
                return {
                    "page_id": page_id,
                    "workspace_id": ws_id,
                    "epoch_id": epoch["id"],
                    "epoch_number": epoch["number"],
                    "state": "active",
                }

    def heartbeat(self, ws_id: str, page_id: str) -> dict:
        with self._lock:
            self._maintenance_locked(ws_id)
            page = self._page(page_id)
            if page is None or page["workspace_id"] != ws_id:
                raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
            if page["state"] != "active":
                raise ApiError(409, "page_not_active", "页面已失效或已关闭，请重新载入",
                               {"page_state": page["state"]})
            self.conn.execute("UPDATE pages SET last_seen=? WHERE id=?", (utcnow(), page_id))
            return {"page_id": page_id, "state": "active"}

    def close_page(self, ws_id: str, page_id: str) -> dict:
        with self._lock:
            page = self._page(page_id)
            if page is None or page["workspace_id"] != ws_id:
                raise ApiError(404, "page_not_found", "页面不存在")
            if page["state"] == "active":
                self.conn.execute(
                    "UPDATE pages SET state='closed', closed_at=? WHERE id=?",
                    (utcnow(), page_id),
                )
            # 页面关闭可能让迁移属主失联：立即按持久化阶段恢复
            self._maintenance_locked(ws_id)
            page = self._page(page_id)
            return {"page_id": page_id, "state": page["state"]}

    # ---------------------------------------------------------------- 记录

    def add_record(self, ws_id: str, page_id: str, content: str) -> dict:
        """写入记录。发布前后旧页面的迟到保存一律被拒绝并提示重新载入。"""
        content = (content or "").strip()
        if not content:
            raise ApiError(400, "bad_content", "记录内容不能为空")
        if len(content) > 2000:
            raise ApiError(400, "bad_content", "记录内容过长")
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                page = self._page(page_id)
                if page is None or page["workspace_id"] != ws_id:
                    raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
                if page["state"] != "active":
                    raise ApiError(409, "page_not_active",
                                   "本页面已失效，保存被拒绝，请重新载入",
                                   {"page_state": page["state"]})
                if ws["migration_phase"] in ACTIVE_PHASES:
                    raise ApiError(409, "migration_in_progress",
                                   f"迁移进行中（{ws['migration_phase']}），保存被拒绝，请重新载入或稍后重试")
                if page["epoch_id"] != ws["current_epoch_id"]:
                    raise ApiError(409, "stale_epoch",
                                   "工作区已切换到新纪元，本次保存被拒绝，请重新载入")
                seq = self.conn.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS s FROM records WHERE epoch_id=?",
                    (ws["current_epoch_id"],),
                ).fetchone()["s"]
                rec_id = new_id("rec")
                self.conn.execute(
                    "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (rec_id, ws_id, ws["current_epoch_id"], seq, content, utcnow()),
                )
                return {"id": rec_id, "seq": seq, "content": content,
                        "epoch_id": ws["current_epoch_id"]}

    # ---------------------------------------------------------------- 迁移

    def start_migration(self, ws_id: str, page_id: str, target_version: str) -> dict:
        """发起目标版本迁移：创建唯一候选纪元并进入复制阶段。"""
        target_version = (target_version or "").strip()
        if not target_version:
            raise ApiError(400, "bad_version", "目标版本不能为空")
        if len(target_version) > 40:
            raise ApiError(400, "bad_version", "目标版本过长")
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] in ACTIVE_PHASES:
                    raise ApiError(409, "migration_active",
                                   "已有进行中的迁移，不会创建第二个候选纪元")
                self._require_driver(ws, self._page(page_id))
                # 上一次 failed 遗留的候选先回收
                old_cand = ws["migration_candidate_epoch_id"]
                if old_cand:
                    self.conn.execute(
                        "DELETE FROM record_lineage WHERE target_epoch_id=?", (old_cand,))
                    self.conn.execute("DELETE FROM records WHERE epoch_id=?", (old_cand,))
                    self.conn.execute("DELETE FROM epochs WHERE id=?", (old_cand,))
                number = self.conn.execute(
                    "SELECT COALESCE(MAX(number),0)+1 AS n FROM epochs WHERE workspace_id=?",
                    (ws_id,),
                ).fetchone()["n"]
                cand_id = new_id("ep")
                now = utcnow()
                self.conn.execute(
                    "INSERT INTO epochs(id,workspace_id,number,version,kind,created_at) "
                    "VALUES(?,?,?,?,'candidate',?)",
                    (cand_id, ws_id, number, target_version, now),
                )
                total = self.conn.execute(
                    "SELECT COUNT(*) AS c FROM records WHERE epoch_id=?",
                    (ws["current_epoch_id"],),
                ).fetchone()["c"]
                self.conn.execute(
                    "UPDATE workspaces SET migration_phase='copying', migration_target_version=?, "
                    "migration_candidate_epoch_id=?, migration_source_epoch_id=?, "
                    "migration_owner_page_id=?, migration_copied=0, migration_total=?, "
                    "migration_started_at=?, migration_updated_at=?, migration_error=NULL "
                    "WHERE id=?",
                    (target_version, cand_id, ws["current_epoch_id"], page_id,
                     total, now, now, ws_id),
                )
        return self.get_state(ws_id)

    def copy_batch(self, ws_id: str, page_id: str, batch_size: int = 1) -> dict:
        """复制一批记录到候选纪元；全部复制完自动进入校验阶段。"""
        try:
            batch_size = int(batch_size)
        except (TypeError, ValueError):
            batch_size = 1
        batch_size = max(1, min(batch_size, 500))
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "copying":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能复制")
                self._require_driver(ws, self._page(page_id))
                # 继续驱动的页面认领迁移属主（用于崩溃检测）
                self.conn.execute(
                    "UPDATE workspaces SET migration_owner_page_id=? WHERE id=?",
                    (page_id, ws_id))
                src = ws["migration_source_epoch_id"]
                cand = ws["migration_candidate_epoch_id"]
                copied = ws["migration_copied"]
                rows = self.conn.execute(
                    "SELECT seq, content, created_at FROM records "
                    "WHERE epoch_id=? ORDER BY seq LIMIT ? OFFSET ?",
                    (src, batch_size, copied),
                ).fetchall()
                for r in rows:
                    self.conn.execute(
                        "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (new_id("rec"), ws_id, cand, r["seq"], r["content"], r["created_at"]),
                    )
                copied += len(rows)
                total = ws["migration_total"]
                new_phase = "validating" if copied >= total else "copying"
                self.conn.execute(
                    "UPDATE workspaces SET migration_copied=?, migration_phase=?, "
                    "migration_updated_at=? WHERE id=?",
                    (copied, new_phase, utcnow(), ws_id),
                )
        return self.get_state(ws_id)

    def validate_migration(self, ws_id: str, page_id: str) -> dict:
        """校验候选纪元与源纪元一致；通过则进入待发布阶段。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "validating":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能校验")
                self._require_driver(ws, self._page(page_id))
                # 认领迁移属主，避免被维护逻辑误判为属主失联
                self.conn.execute(
                    "UPDATE workspaces SET migration_owner_page_id=? WHERE id=?",
                    (page_id, ws_id))
                src_sum = self._checksum(ws["migration_source_epoch_id"])
                cand_sum = self._checksum(ws["migration_candidate_epoch_id"])
                now = utcnow()
                if src_sum == cand_sum:
                    self.conn.execute(
                        "UPDATE workspaces SET migration_phase='publishing', "
                        "migration_error=NULL, migration_updated_at=? WHERE id=?",
                        (now, ws_id),
                    )
                else:
                    self.conn.execute(
                        "UPDATE workspaces SET migration_phase='failed', migration_error=?, "
                        "migration_updated_at=? WHERE id=?",
                        (f"校验失败：源 {src_sum[0]} 条 / 候选 {cand_sum[0]} 条或内容摘要不一致",
                         now, ws_id),
                    )
        return self.get_state(ws_id)

    def publish_migration(self, ws_id: str, page_id: str) -> dict:
        """原子发布：单事务完成指针切换、旧纪元作废、旧页面失效。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "publishing":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能发布")
                self._require_driver(ws, self._page(page_id))
                if not self._publish_locked(ws):
                    raise ApiError(409, "validation_mismatch",
                                   "发布前复核不一致，迁移已标记失败")
        return self.get_state(ws_id)

    def _publish_locked(self, ws: sqlite3.Row) -> bool:
        """发布事务体（恢复路径与 publish API 共用）。返回是否成功。"""
        old = ws["current_epoch_id"]
        cand = ws["migration_candidate_epoch_id"]
        if not cand:
            return False
        now = utcnow()
        # 发布前复核：候选必须与源纪元一致，否则标记失败而不是发布半成品
        if self._checksum(old) != self._checksum(cand):
            self.conn.execute(
                "UPDATE workspaces SET migration_phase='failed', migration_error=?, "
                "migration_updated_at=? WHERE id=?",
                ("发布前复核不一致，已中止发布", now, ws["id"]),
            )
            return False
        # 不可变映射随指针切换在同一持久化提交中固化：按记录出现顺序（seq）逐位
        # 配对，重复正文也各自成行；先落映射，再切指针，外界要么看不到要么看到全套。
        self._freeze_lineage_locked(old, cand, now)
        self.conn.execute("UPDATE epochs SET kind='superseded' WHERE id=?", (old,))
        self.conn.execute("UPDATE epochs SET kind='published' WHERE id=?", (cand,))
        # 持有旧纪元围栏的页面全部失效：其后的迟到保存会被拒绝
        self.conn.execute(
            "UPDATE pages SET state='invalidated', invalidated_at=? "
            "WHERE workspace_id=? AND state='active' AND epoch_id=?",
            (now, ws["id"], old),
        )
        self.conn.execute(
            "UPDATE workspaces SET current_epoch_id=?, migration_phase='published', "
            "migration_candidate_epoch_id=NULL, migration_owner_page_id=NULL, "
            "migration_error=NULL, migration_updated_at=? WHERE id=?",
            (cand, now, ws["id"]),
        )
        return True

    def _freeze_lineage_locked(self, source_epoch_id: str, target_epoch_id: str, now: str):
        """在发布事务内把源→目标记录按出现顺序逐位固化为不可变映射。

        重复正文不做去重：两侧均按 seq 排序后按下标配对，每条记录独立成行；
        发布前复核已保证两侧 (条数, 摘要) 完全一致，此处再以条数断言兜底。
        """
        src_rows = self.conn.execute(
            "SELECT id, seq FROM records WHERE epoch_id=? ORDER BY seq",
            (source_epoch_id,),
        ).fetchall()
        dst_rows = self.conn.execute(
            "SELECT id, seq FROM records WHERE epoch_id=? ORDER BY seq",
            (target_epoch_id,),
        ).fetchall()
        if len(src_rows) != len(dst_rows):
            raise ApiError(500, "lineage_count_mismatch",
                           "固化谱系时两侧记录数不一致，已中止发布")
        h = hashlib.sha256()
        for src, dst in zip(src_rows, dst_rows):
            # 复制保留 seq：出现顺序的位置一一对应（重复正文也分别保留）
            self.conn.execute(
                "INSERT INTO record_lineage(target_epoch_id,target_seq,target_record_id,"
                "source_epoch_id,source_seq,source_record_id,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (target_epoch_id, dst["seq"], dst["id"],
                 source_epoch_id, src["seq"], src["id"], now),
            )
            h.update(f"{dst['seq']}={src['seq']}\n".encode())
        self.conn.execute(
            "UPDATE epochs SET source_epoch_id=?, lineage_frozen_at=?, lineage_count=?, "
            "lineage_digest=? WHERE id=?",
            (source_epoch_id, now, len(dst_rows), h.hexdigest(), target_epoch_id),
        )

    # ---------------------------------------------------------------- 状态

    def get_state(self, ws_id: str, touch_page_id: str | None = None) -> dict:
        """页面展示所需的全部状态：当前纪元、迁移阶段、围栏页面及失效状态、当前纪元记录。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            if touch_page_id:
                self.conn.execute(
                    "UPDATE pages SET last_seen=? WHERE id=? AND workspace_id=? AND state='active'",
                    (utcnow(), touch_page_id, ws_id),
                )
            ws = self._ws(ws_id)
            cur = self._epoch(ws["current_epoch_id"])
            records = self.conn.execute(
                "SELECT id, seq, content, created_at FROM records "
                "WHERE epoch_id=? ORDER BY seq",
                (ws["current_epoch_id"],),
            ).fetchall()
            pages = self.conn.execute(
                "SELECT p.id, p.epoch_id, p.state, p.created_at, p.last_seen, "
                "       p.closed_at, p.invalidated_at, e.number AS epoch_number "
                "FROM pages p LEFT JOIN epochs e ON e.id = p.epoch_id "
                "WHERE p.workspace_id=? ORDER BY p.created_at DESC, p.id DESC LIMIT 50",
                (ws_id,),
            ).fetchall()
            migration = None
            if ws["migration_started_at"] is not None:
                cand = self._epoch(ws["migration_candidate_epoch_id"])
                migration = {
                    "phase": ws["migration_phase"],
                    "target_version": ws["migration_target_version"],
                    "candidate_epoch": (
                        {"id": cand["id"], "number": cand["number"]} if cand else None
                    ),
                    "source_epoch_id": ws["migration_source_epoch_id"],
                    "owner_page_id": ws["migration_owner_page_id"],
                    "copied": ws["migration_copied"],
                    "total": ws["migration_total"],
                    "started_at": ws["migration_started_at"],
                    "updated_at": ws["migration_updated_at"],
                    "error": ws["migration_error"],
                }
            return {
                "id": ws["id"],
                "name": ws["name"],
                "created_at": ws["created_at"],
                "current_epoch": {
                    "id": cur["id"],
                    "number": cur["number"],
                    "version": cur["version"],
                    "record_count": len(records),
                },
                "migration": migration,
                "pages": [dict(p) for p in pages],
                "records": [dict(r) for r in records],
            }

    # ---------------------------------------------------------------- 纪元与谱系

    def list_epochs(self, ws_id: str) -> dict:
        """列出当前工作区全部已发布纪元（含已被取代的历史纪元）。

        候选纪元（复制中/校验中/失败/回收）一律不在其列，绝不暴露部分映射。
        """
        with self._lock:
            self._maintenance_locked(ws_id)
            ws = self._ws(ws_id)
            rows = self.conn.execute(
                "SELECT e.id, e.number, e.version, e.kind, e.created_at, "
                "       e.source_epoch_id, e.lineage_frozen_at, e.lineage_count, "
                "       se.number AS source_epoch_number, se.version AS source_epoch_version, "
                "       (SELECT COUNT(*) FROM records r WHERE r.epoch_id=e.id) AS record_count "
                "FROM epochs e LEFT JOIN epochs se ON se.id=e.source_epoch_id "
                "WHERE e.workspace_id=? AND e.kind IN ('published','superseded') "
                "ORDER BY e.number",
                (ws_id,),
            ).fetchall()
            return {
                "workspace_id": ws_id,
                "current_epoch_id": ws["current_epoch_id"],
                "epochs": [
                    {
                        "id": r["id"],
                        "number": r["number"],
                        "version": r["version"],
                        "kind": r["kind"],
                        "is_current": r["id"] == ws["current_epoch_id"],
                        "record_count": r["record_count"],
                        "created_at": r["created_at"],
                        "has_migration_source": r["source_epoch_id"] is not None,
                        "source_epoch": (
                            {"id": r["source_epoch_id"],
                             "number": r["source_epoch_number"],
                             "version": r["source_epoch_version"]}
                            if r["source_epoch_id"] else None
                        ),
                        "lineage_frozen_at": r["lineage_frozen_at"],
                        "lineage_count": r["lineage_count"],
                    }
                    for r in rows
                ],
            }

    def get_lineage(self, ws_id: str, epoch_id: str) -> dict:
        """返回某已发布纪元的逐条谱系：目标序号、源序号、两侧稳定摘要、来源说明。

        仅已发布（含已被取代）且谱系凭据完整的纪元可返回；候选/中断/回收纪元
        返回 404，绝不暴露部分映射。旧版起始纪元明确标示为无迁移来源，
        其记录来源说明为「起始纪元原始记录」；发布后在本纪元新建的记录标示为
        「本纪元新建」。
        """
        with self._lock:
            self._maintenance_locked(ws_id)
            ws = self._ws(ws_id)
            ep = self._epoch(epoch_id)
            if ep is None or ep["workspace_id"] != ws_id or ep["kind"] not in (
                "published", "superseded"
            ):
                raise ApiError(404, "epoch_lineage_unavailable",
                               "纪元不存在或尚未发布，谱系不可用")

            target_records = self.conn.execute(
                "SELECT id, seq, content, created_at FROM records WHERE epoch_id=? ORDER BY seq",
                (epoch_id,),
            ).fetchall()

            source = None
            source_by_id: dict[str, sqlite3.Row] = {}
            if ep["source_epoch_id"] is not None:
                # 凭据完整性：来源纪元与发布事务固化的时间/摘要必须齐备
                if ep["lineage_frozen_at"] is None or ep["lineage_digest"] is None:
                    raise ApiError(409, "lineage_credentials_incomplete",
                                   "谱系凭据不完整，拒绝返回映射")
                source = self._epoch(ep["source_epoch_id"])
                if source is None:
                    raise ApiError(409, "lineage_credentials_incomplete",
                                   "谱系来源纪元缺失，拒绝返回映射")
                frozen_rows = self.conn.execute(
                    "SELECT * FROM record_lineage WHERE target_epoch_id=? ORDER BY target_seq",
                    (epoch_id,),
                ).fetchall()
                if len(frozen_rows) != ep["lineage_count"]:
                    raise ApiError(409, "lineage_credentials_incomplete",
                                   "固化映射条数与凭据不符，拒绝返回映射")
                frozen_by_target = {r["target_record_id"]: r for r in frozen_rows}
                src_records = self.conn.execute(
                    "SELECT id, seq, content FROM records WHERE epoch_id=?",
                    (source["id"],),
                ).fetchall()
                source_by_id = {r["id"]: r for r in src_records}
            else:
                frozen_by_target = {}

            entries = []
            for r in target_records:
                link = frozen_by_target.get(r["id"])
                if link is not None:
                    src = source_by_id.get(link["source_record_id"])
                    if src is None:
                        raise ApiError(409, "lineage_credentials_incomplete",
                                       "映射指向的源记录缺失，拒绝返回映射")
                    entries.append({
                        "origin": "migrated",
                        "origin_label": f"由纪元 #{source['number']}（{source['version']}）迁移而来",
                        "target_seq": r["seq"],
                        "target_record_id": r["id"],
                        "target_digest": self._record_digest(r["content"]),
                        "target_content": r["content"],
                        "source_seq": src["seq"],
                        "source_record_id": src["id"],
                        "source_digest": self._record_digest(src["content"]),
                        "source_content": src["content"],
                    })
                elif ep["source_epoch_id"] is None:
                    entries.append({
                        "origin": "origin",
                        "origin_label": "起始纪元原始记录（无迁移来源）",
                        "target_seq": r["seq"],
                        "target_record_id": r["id"],
                        "target_digest": self._record_digest(r["content"]),
                        "target_content": r["content"],
                        "source_seq": None,
                        "source_record_id": None,
                        "source_digest": None,
                        "source_content": None,
                    })
                else:
                    # 发布之后在本纪元新建的记录：谱系中不存在映射行
                    entries.append({
                        "origin": "created",
                        "origin_label": "本纪元新建（发布后录入，无迁移来源）",
                        "target_seq": r["seq"],
                        "target_record_id": r["id"],
                        "target_digest": self._record_digest(r["content"]),
                        "target_content": r["content"],
                        "source_seq": None,
                        "source_record_id": None,
                        "source_digest": None,
                        "source_content": None,
                    })

            return {
                "workspace_id": ws_id,
                "epoch": {
                    "id": ep["id"],
                    "number": ep["number"],
                    "version": ep["version"],
                    "kind": ep["kind"],
                    "is_current": ep["id"] == ws["current_epoch_id"],
                    "record_count": len(target_records),
                    "has_migration_source": ep["source_epoch_id"] is not None,
                },
                "source_epoch": (
                    {"id": source["id"], "number": source["number"], "version": source["version"]}
                    if source is not None else None
                ),
                "lineage_frozen_at": ep["lineage_frozen_at"],
                "lineage_count": ep["lineage_count"],
                "lineage_digest": ep["lineage_digest"],
                "entries": entries,
            }
