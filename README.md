# 野外实验站 · 离线记录页（纪元迁移版）

离线记录页的索引结构升级方案：以**纪元（epoch）**为单位组织记录，迁移先把旧数据
复制到**候选纪元**并校验，再**原子发布**新纪元。停留在旧页面的录入员的迟到保存
在发布前后一律被拒绝并提示重新载入；重开后只能读到新纪元的完整记录。

仅依赖 Python 3 标准库（sqlite3 / http.server），适配离线环境。

## 运行

```bash
# 编排环境（推荐）
docker compose up --build app          # 记录页服务，http://localhost:8000

# 本地（无 Docker）
make local                             # 或 DB_PATH=./data/app.db python3 -m app.server
```

打开两个浏览器标签页即两个"页面"，各自持有纪元围栏；页面展示**当前纪元、
迁移阶段、持有围栏的页面及各页面失效状态**。

## 验收（verify 服务）

```bash
make verify          # = docker compose up --build --exit-code-from verify --abort-on-container-exit
# 或
sh scripts/verify.sh
```

`verify` 服务在编排网络内穿插执行：构建检查（编译/导入/静态资源）→ 单元测试 →
健康与页面 HTTP 冒烟 → API 冒烟 → 场景一（两页面迁移 + 旧写拒绝 + 并发迁移不建
第二候选）→ 场景二（复制中断重开不展示部分数据 + 校验阶段续用同一候选）→
场景三（已发布纪元选择与逐行谱系：按出现顺序固化、重复正文分别保留、候选/回收/
未发布纪元不暴露映射、起始纪元无迁移来源）→ 场景四（`POST /api/admin/shutdown`
触发进程退出，编排层按 `restart: unless-stopped` 拉起后，校验本地恢复的
纪元/记录/失效状态及历史谱系一致）。跑完自行退出，退出码即结果。

本地无编排时：`python3 verify.py --no-restart`（跳过重启场景）。

## 迁移协议与恢复规则

```
idle ──发起迁移──▶ copying ──复制完成──▶ validating ──校验通过──▶ publishing ──原子发布──▶ published
                    │                       │                      │
   页面在此关闭 ──▶ 安全回收候选(aborted)   续用同一候选            恢复时补齐发布
```

- **读取**：永远只来自工作区指针指向的已发布纪元；候选纪元从不对外读取，
  因此读结果只能是完整旧纪元或完整新纪元。
- **并发**：迁移状态是工作区行上的唯一槽位，并发发起返回 409，不会创建第二个候选纪元。
- **发布**：单事务完成指针切换、旧纪元作废、旧纪元围栏页面全部失效。
- **迟到保存**：迁移进行中（发布前）与页面失效/纪元切换后（发布后），写入一律
  409 并提示"请重新载入"。
- **恢复**：页面在复制/校验/发布之间关闭（显式关闭、心跳过期或进程重启），
  由后来页面或下次启动依据持久化阶段恢复：copying→回收候选；validating→保留
  同一候选待续；publishing→补齐发布。

## 谱系（lineage）复核

发布事务在切换纪元指针的**同一持久化提交**中固化两类凭据：

- `epoch_origins`：每个迁移纪元一行的来源凭据（目标纪元←源纪元）。空记录纪元迁移
  也保留凭据；旧版起始纪元在此表无行，谱系中明确标示为「无迁移来源」。
- `lineage_mappings`：按记录出现顺序逐行建立的「目标序号 ↔ 源序号」不可变映射，
  以**位置（seq）而非正文**连接，因此正文相同的独立记录也各自成行，不会在复核时
  被误认成同一条。

谱系访问规则：

- **可查**：仅已发布（`published`，含历史 `superseded`）且凭据完整的目标纪元。
- **拒绝**：未发布候选、复制中断后回收、校验失败的纪元不暴露任何映射片段（409/404）。
- **新建记录**：迁移发布后在目标纪元录入的观测无映射行，标示为「本纪元创建」。
- **持久性**：重开页面或 Compose 重启应用后，当前记录、历史已发布纪元及其谱系一致。

复核员可在状态页「纪元选择与谱系查看」面板选择任一已发布纪元，表格逐行展示
目标序号、目标稳定摘要（sha256 前 12 位）、源序号、源稳定摘要及来源说明
（起始纪元创建 / 自源纪元 #N 位置 #M 迁移而来 / 本纪元创建（迁移后新录入））。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查（含 DB） |
| GET/POST | `/api/workspaces` | 列出 / 建立工作区 |
| GET | `/api/workspaces/{id}/state` | 当前纪元、迁移阶段、围栏页面及失效状态、当前纪元记录 |
| GET | `/api/workspaces/{id}/epochs` | 列出可复核的已发布纪元（候选不出现） |
| GET | `/api/workspaces/{id}/epochs/{eid}/lineage` | 逐行谱系（目标/源序号、两侧稳定摘要、来源说明） |
| POST | `/api/workspaces/{id}/pages` | 打开页面（在当前纪元建立围栏） |
| POST | `/api/workspaces/{id}/pages/{pid}/heartbeat` | 心跳 |
| POST/DELETE | `/api/workspaces/{id}/pages/{pid}/close` | 关闭页面 |
| POST | `/api/workspaces/{id}/records` | 写记录（校验围栏/失效/迁移中） |
| POST | `.../migration/start` `copy` `validate` `publish` | 迁移四步 |
| POST | `/api/admin/shutdown` | 进程退出（需 `ALLOW_ADMIN_SHUTDOWN=1`，仅供编排验收） |

## 目录

```
app/db.py        纪元/迁移状态机存储层（SQLite，WAL）
app/server.py    HTTP API + 静态页
app/static/      记录页前端
tests/           单元测试（12 例）
verify.py        编排内验收服务（退出码报告结果）
docker-compose.yml / Dockerfile / scripts/verify.sh
```
