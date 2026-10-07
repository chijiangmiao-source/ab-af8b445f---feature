#!/usr/bin/env python3
"""verify 服务：在编排环境内完成全部验收并自行退出，以退出码报告结果。

步骤穿插执行：构建检查 -> 代码测试 -> 健康/页面 HTTP 冒烟 -> API 冒烟 ->
场景一（两页面迁移 + 旧写拒绝 + 并发迁移不建第二候选）->
场景二（复制中断重开不展示部分数据 + 校验阶段续用同一候选）->
场景三（发布后进程重启，本地恢复的纪元/记录/失效状态一致）。

用法：
  python verify.py [--base-url http://app:8000] [--no-restart]
"""

from __future__ import annotations

import argparse
import compileall
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent

RESULTS: list[tuple[str, bool, str]] = []


class VerifyError(Exception):
    pass


def step(name):
    """注册并包装一个验收步骤。"""
    def deco(fn):
        def wrapped(ctx):
            print(f"\n[verify] === {name} ===", flush=True)
            try:
                fn(ctx)
            except Exception as e:  # noqa: BLE001
                RESULTS.append((name, False, str(e)))
                print(f"[verify] ✗ {name}: {e}", flush=True)
            else:
                RESULTS.append((name, True, ""))
                print(f"[verify] ✓ {name}", flush=True)
        return wrapped
    return deco


def check(cond, msg):
    if not cond:
        raise VerifyError(msg)


# ---------------------------------------------------------------- HTTP 工具

def req(ctx, method, path, body=None, expect=None):
    url = ctx["base"] + path
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    obj = json.loads(payload) if payload else {}
    if expect is not None and code != expect:
        raise VerifyError(f"{method} {path} 期望 {expect} 实际 {code}: {obj}")
    return code, obj


def wait_health(ctx, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, obj = req(ctx, "GET", "/healthz")
            if code == 200 and obj.get("ok"):
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(1)
    raise VerifyError(f"等待健康检查超时（{timeout}s）")


# ---------------------------------------------------------------- 构建与测试

@step("构建检查：Python 编译、模块导入与静态资源完整性")
def build_check(ctx):
    ok = compileall.compile_dir(str(ROOT / "app"), quiet=1, force=True)
    check(ok, "app/ 编译失败")
    ok = compileall.compile_dir(str(ROOT / "tests"), quiet=1, force=True)
    check(ok, "tests/ 编译失败")
    ok = compileall.compile_file(str(ROOT / "verify.py"), quiet=1, force=True)
    check(ok, "verify.py 编译失败")
    for rel in ("app/static/index.html", "app/static/app.js", "app/static/style.css"):
        check((ROOT / rel).exists(), f"缺少 {rel}")
    html = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    check("/app.js" in html and "/style.css" in html, "index.html 未引用静态资源")
    sys.path.insert(0, str(ROOT))
    import app.db  # noqa: F401
    import app.server  # noqa: F401


@step("代码测试：纪元迁移单元测试")
def unit_tests(ctx):
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
        cwd=ROOT, capture_output=True, text=True, timeout=120)
    sys.stdout.write(proc.stdout[-2000:])
    sys.stdout.write(proc.stderr[-2000:])
    check(proc.returncode == 0, "单元测试失败")


# ---------------------------------------------------------------- 冒烟

@step("HTTP 冒烟：健康响应")
def smoke_health(ctx):
    wait_health(ctx)
    code, obj = req(ctx, "GET", "/healthz", expect=200)
    check(obj.get("ok") is True and obj.get("db") == "up", f"健康响应异常: {obj}")


@step("HTTP 冒烟：页面与静态资源")
def smoke_pages(ctx):
    # 页面是 HTML，直接原始读取而非 JSON 解析
    with urllib.request.urlopen(ctx["base"] + "/", timeout=10) as resp:
        text = resp.read().decode()
        check(resp.status == 200, "首页状态码非 200")
    check("离线记录页" in text, "首页缺少应用标识")
    for asset in ("/app.js", "/style.css"):
        with urllib.request.urlopen(ctx["base"] + asset, timeout=10) as resp:
            check(resp.status == 200, f"{asset} 状态码非 200")
            check(len(resp.read()) > 100, f"{asset} 内容异常")


@step("API 冒烟：建工作区、开页面、写记录、读状态")
def smoke_api(ctx):
    ws_name = f"verify-{uuid.uuid4().hex[:8]}"
    code, ws = req(ctx, "POST", "/api/workspaces", {"name": ws_name}, expect=201)
    ctx["ws"] = ws["id"]
    check(ws["current_epoch"]["number"] == 1, "初始纪元应为 #1")
    code, page = req(ctx, "POST", f"/api/workspaces/{ws['id']}/pages", expect=201)
    ctx["page_a"] = page["page_id"]
    for i in range(5):
        req(ctx, "POST", f"/api/workspaces/{ws['id']}/records",
            {"page_id": page["page_id"], "content": f"观测记录-{i + 1}"}, expect=201)
    code, s = req(ctx, "GET", f"/api/workspaces/{ws['id']}/state", expect=200)
    check(len(s["records"]) == 5, "记录数应为 5")
    check(s["current_epoch"]["record_count"] == 5, "纪元计数应为 5")


@step("API 冒烟：起始纪元列表与无迁移来源谱系")
def smoke_lineage_origin(ctx):
    ws = ctx["ws"]
    _, data = req(ctx, "GET", f"/api/workspaces/{ws}/epochs", expect=200)
    check(len(data["epochs"]) == 1, "发布前应只有起始纪元一个已发布纪元")
    ep = data["epochs"][0]
    check(ep["number"] == 1 and ep["is_current"] is True, "起始纪元应为当前纪元")
    check(ep["has_migration_source"] is False, "起始纪元应标示无迁移来源")
    _, lin = req(ctx, "GET", f"/api/workspaces/{ws}/epochs/{ep['id']}/lineage", expect=200)
    check(lin["source_epoch"] is None, "起始纪元谱系不应有源纪元")
    check(lin["lineage_frozen_at"] is None, "起始纪元不应有固化时间")
    check([e["origin"] for e in lin["entries"]] == ["origin"] * 5,
          "起始纪元每条记录都应标示为原始记录")
    check(all(e["source_seq"] is None and e["source_digest"] is None for e in lin["entries"]),
          "原始记录不应有源序号/源摘要")
    check(all(e["target_digest"] and "无迁移来源" in e["origin_label"]
              for e in lin["entries"]), "每条记录应有稳定摘要与无来源说明")
    # 不存在的纪元 -> 404，且不泄露任何信息
    code, err = req(ctx, "GET", f"/api/workspaces/{ws}/epochs/ep_nonexistent/lineage")
    check(code == 404 and err.get("error") == "epoch_lineage_unavailable",
          f"未知纪元谱系应 404: {code} {err}")


# ---------------------------------------------------------------- 场景一

@step("场景一：两页面迁移，旧页迟到保存在发布前后均被拒绝")
def scenario_stale_write_rejected(ctx):
    ws = ctx["ws"]
    page_b = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_b"] = page_b

    # 发起迁移；并发发起第二个迁移必须被拒绝且不产生第二候选
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
               {"page_id": ctx["page_a"], "target_version": "v2"}, expect=200)
    check(s["migration"]["phase"] == "copying", "迁移应进入复制阶段")
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
                    {"page_id": page_b, "target_version": "v2b"})
    check(code == 409 and err.get("error") == "migration_active",
          f"并发迁移应被拒绝: {code} {err}")
    check(s["migration"]["candidate_epoch"] is not None, "应存在唯一候选纪元")

    # 发布前：另一页的保存被拒绝
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/records",
                    {"page_id": page_b, "content": "迟到记录"})
    check(code == 409 and err.get("error") == "migration_in_progress",
          f"发布前旧写应被拒绝: {code} {err}")

    # 复制 -> 校验 -> 原子发布
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
                   {"page_id": ctx["page_a"], "batch_size": 2}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/validate",
        {"page_id": ctx["page_a"]}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/publish",
               {"page_id": ctx["page_a"]}, expect=200)
    check(s["migration"]["phase"] == "published", "迁移应已发布")
    check(s["current_epoch"]["number"] == 2, "当前纪元应为 #2")
    check(s["current_epoch"]["version"] == "v2", "当前版本应为 v2")
    check([r["content"] for r in s["records"]] == [f"观测记录-{i}" for i in range(1, 6)],
          "新纪元记录应完整")

    # 发布后：仍停留在旧页的迟到保存被拒绝并提示重新载入
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/records",
                    {"page_id": page_b, "content": "迟到记录"})
    check(code == 409 and "重新载入" in err.get("message", ""),
          f"发布后旧写应被拒绝并提示重新载入: {code} {err}")

    # 重开页面：只能读到新纪元完整记录，且可继续写入
    page_c = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_c"] = page_c
    req(ctx, "POST", f"/api/workspaces/{ws}/records",
        {"page_id": page_c, "content": "新纪元记录"}, expect=201)
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(len(s["records"]) == 6, "新纪元应有 6 条记录")
    states = {p["id"]: p["state"] for p in s["pages"]}
    check(states[ctx["page_a"]] == "invalidated", "页面A应已失效")
    check(states[page_b] == "invalidated", "页面B应已失效")
    check(states[page_c] == "active", "页面C应为活动")


# ---------------------------------------------------------------- 场景二

@step("场景二：复制中断重开不展示部分数据；校验阶段续用同一候选")
def scenario_interruption_recovery(ctx):
    ws = ctx["ws"]
    page_c = ctx["page_c"]

    # —— 复制中断：复制一部分后关闭负责页面 ——
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
        {"page_id": page_c, "target_version": "v3"}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
               {"page_id": page_c, "batch_size": 1}, expect=200)
    check(s["migration"]["copied"] == 1 and s["migration"]["phase"] == "copying",
          "应处于部分复制状态")
    recycled_candidate = s["migration"]["candidate_epoch"]["id"]
    # 复制中的候选绝不能暴露谱系（部分映射不得外泄）
    code, err = req(ctx, "GET", f"/api/workspaces/{ws}/epochs/{recycled_candidate}/lineage")
    check(code == 404 and err.get("error") == "epoch_lineage_unavailable",
          f"复制中的候选谱系应 404: {code} {err}")
    req(ctx, "POST", f"/api/workspaces/{ws}/pages/{page_c}/close", expect=200)

    # 重开页面触发恢复：候选被安全回收，读取仍是完整旧纪元
    page_d = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(s["migration"]["phase"] == "aborted",
          f"复制中断应被安全回收: {s['migration']['phase']}")
    check(s["migration"]["candidate_epoch"] is None, "候选应已回收")
    # 已回收候选仍不得暴露映射
    code, err = req(ctx, "GET", f"/api/workspaces/{ws}/epochs/{recycled_candidate}/lineage")
    check(code == 404 and err.get("error") == "epoch_lineage_unavailable",
          f"已回收候选谱系应 404: {code} {err}")
    code, data = req(ctx, "GET", f"/api/workspaces/{ws}/epochs", expect=200)
    check(all(e["id"] != recycled_candidate for e in data["epochs"]),
          "纪元列表不得出现已回收候选")
    check(s["current_epoch"]["number"] == 2, "仍应读到旧纪元 #2")
    check(len(s["records"]) == 6, "记录应完整（无部分复制数据）")

    # —— 校验阶段中断：后来页面续用同一候选 ——
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
        {"page_id": page_d, "target_version": "v3"}, expect=200)
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
                   {"page_id": page_d, "batch_size": 3}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    cand = s["migration"]["candidate_epoch"]
    req(ctx, "POST", f"/api/workspaces/{ws}/pages/{page_d}/close", expect=200)

    page_e = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_e"] = page_e
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(s["migration"]["phase"] == "validating", "校验阶段应保留待续")
    check(s["migration"]["candidate_epoch"] == cand, "应续用同一候选纪元")
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/validate",
        {"page_id": page_e}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/publish",
               {"page_id": page_e}, expect=200)
    check(s["current_epoch"]["number"] == 3, "当前纪元应为 #3")
    check(s["current_epoch"]["version"] == "v3", "当前版本应为 v3")
    check(len(s["records"]) == 6, "新纪元记录应完整")


# ---------------------------------------------------------------- 谱系

@step("谱系：已发布纪元逐条映射、重复正文分别保留、本纪元新建标示")
def scenario_lineage(ctx):
    # 复用场景一的工作区：纪元 #2 = 5 条迁移记录 + 1 条发布后新建
    ws = ctx["ws"]
    _, data = req(ctx, "GET", f"/api/workspaces/{ws}/epochs", expect=200)
    nums = sorted(e["number"] for e in data["epochs"])
    check(nums == [1, 2], f"应列出已发布纪元 #1/#2: {nums}")
    by_num = {e["number"]: e for e in data["epochs"]}
    check(by_num[2]["is_current"] is True, "#2 应为当前纪元")
    check(by_num[2]["has_migration_source"] is True
          and by_num[2]["source_epoch"]["number"] == 1, "#2 应标记迁移自 #1")
    check(by_num[1]["has_migration_source"] is False, "#1 应无迁移来源")
    check(by_num[1]["kind"] == "superseded", "#1 应为已被取代状态但仍可查谱系")

    _, lin = req(ctx, "GET", f"/api/workspaces/{ws}/epochs/{by_num[2]['id']}/lineage",
                 expect=200)
    check(lin["lineage_count"] == 5 and len(lin["entries"]) == 6,
          "固化 5 条映射，纪元共 6 条记录")
    check(lin["source_epoch"]["number"] == 1, "源纪元应为 #1")
    check(lin["lineage_frozen_at"] is not None, "应有发布事务内的固化时间戳")
    pairs = [(e["target_seq"], e["source_seq"], e["origin"]) for e in lin["entries"]]
    check(pairs == [(1, 1, "migrated"), (2, 2, "migrated"), (3, 3, "migrated"),
                    (4, 4, "migrated"), (5, 5, "migrated"), (6, None, "created")],
          f"谱系对应关系错误: {pairs}")
    mig = [e for e in lin["entries"] if e["origin"] == "migrated"]
    check(all(e["target_digest"] == e["source_digest"] for e in mig),
          "迁移记录两侧稳定摘要必须一致")
    check(all("迁移而来" in e["origin_label"] for e in mig), "迁移记录来源说明错误")
    check("本纪元新建" in lin["entries"][-1]["origin_label"], "新建记录来源说明错误")
    # 两侧稳定摘要稳定可复算
    import hashlib
    expect_d = hashlib.sha256("观测记录-1".encode()).hexdigest()[:12]
    check(lin["entries"][0]["target_digest"] == expect_d, "稳定摘要算法不符")

    # —— 重复正文：独立工作区内制造正文相同的不同记录 ——
    _, dup_ws = req(ctx, "POST", "/api/workspaces",
                    {"name": f"dup-{uuid.uuid4().hex[:8]}"}, expect=201)
    p = req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/pages", expect=201)[1]["page_id"]
    for content in ("重复观测", "重复观测", "别的观测"):
        req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/records",
            {"page_id": p, "content": content}, expect=201)
    req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/migration/start",
        {"page_id": p, "target_version": "v2"}, expect=200)
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/migration/copy",
                   {"page_id": p, "batch_size": 5}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/migration/validate",
        {"page_id": p}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{dup_ws['id']}/migration/publish",
               {"page_id": p}, expect=200)
    _, lin2 = req(ctx, "GET",
                  f"/api/workspaces/{dup_ws['id']}/epochs/{s['current_epoch']['id']}/lineage",
                  expect=200)
    pairs2 = [(e["target_seq"], e["source_seq"], e["origin"]) for e in lin2["entries"]]
    check(pairs2 == [(1, 1, "migrated"), (2, 2, "migrated"), (3, 3, "migrated")],
          f"重复正文必须分别成行，按出现顺序配对: {pairs2}")
    ids = {e["target_record_id"] for e in lin2["entries"]}
    check(len(ids) == 3, "重复正文的独立记录不得合并")
    check(lin2["entries"][0]["target_digest"] == lin2["entries"][1]["target_digest"],
          "同正文摘要应相同，但序号与记录 id 各自独立")


# ---------------------------------------------------------------- 场景三

@step("场景三：发布后进程重启，本地恢复的纪元/记录/失效状态一致")
def scenario_restart_consistency(ctx):
    if ctx["no_restart"]:
        print("[verify] 跳过（--no-restart）")
        return
    ws = ctx["ws"]
    _, before = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    _, lin_before = req(ctx, "GET",
                        f"/api/workspaces/{ws}/epochs/{before['current_epoch']['id']}/lineage",
                        expect=200)

    code, obj = req(ctx, "POST", "/api/admin/shutdown")
    if code == 403:
        print("[verify] 服务端未启用管理关闭，跳过重启场景")
        return
    check(code == 200, f"关闭请求失败: {code} {obj}")
    time.sleep(1)
    wait_health(ctx, timeout=90)  # 编排层按重启策略拉起

    _, after = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(after["current_epoch"] == before["current_epoch"],
          f"纪元不一致: {after['current_epoch']} != {before['current_epoch']}")
    check([r["content"] for r in after["records"]] ==
          [r["content"] for r in before["records"]], "记录不一致")
    check(after["migration"]["phase"] == "published", "迁移阶段应为已发布")
    inv_before = {p["id"] for p in before["pages"] if p["state"] == "invalidated"}
    inv_after = {p["id"] for p in after["pages"] if p["state"] == "invalidated"}
    check(inv_before and inv_before <= inv_after, "失效页面状态不一致")

    # 重启后历史已发布纪元及其谱系保持一致
    _, epochs = req(ctx, "GET", f"/api/workspaces/{ws}/epochs", expect=200)
    nums = sorted(e["number"] for e in epochs["epochs"])
    check(nums == [1, 2, 3], f"重启后历史纪元应完整: {nums}")
    _, lin_restart = req(ctx, "GET",
                         f"/api/workspaces/{ws}/epochs/{after['current_epoch']['id']}/lineage",
                         expect=200)
    check(lin_restart["lineage_digest"] == lin_before["lineage_digest"],
          "重启后谱系摘要应一致")
    check([(e["target_seq"], e["source_seq"], e["origin"]) for e in lin_restart["entries"]]
          == [(e["target_seq"], e["source_seq"], e["origin"]) for e in lin_before["entries"]],
          "重启后逐条谱系应一致")


# ---------------------------------------------------------------- 主流程

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument("--no-restart", action="store_true",
                        help="跳过进程重启场景（本地无编排重启策略时使用）")
    args = parser.parse_args()
    ctx = {"base": args.base_url.rstrip("/"), "no_restart": args.no_restart}

    print(f"[verify] 目标 {ctx['base']}", flush=True)
    steps = [build_check, unit_tests, smoke_health, smoke_pages, smoke_api,
             smoke_lineage_origin, scenario_stale_write_rejected,
             scenario_lineage, scenario_interruption_recovery,
             scenario_restart_consistency]
    for s in steps:
        s(ctx)

    print("\n[verify] ================= 结果汇总 =================", flush=True)
    failed = 0
    for name, ok, err in RESULTS:
        print(f"[verify] {'PASS' if ok else 'FAIL'}  {name}" +
              (f"  —— {err}" if err else ""), flush=True)
        failed += 0 if ok else 1
    print(f"[verify] {len(RESULTS) - failed}/{len(RESULTS)} 步通过", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
