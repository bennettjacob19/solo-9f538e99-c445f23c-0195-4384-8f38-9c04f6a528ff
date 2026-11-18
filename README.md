# 制品目录历史查询服务（含连续封存与网页核验）

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

本版新增**连续封存（哈希链）与网页核验**：首次启用即封存已有历史（仅证明
此后未变），此后每笔源事务的提交点、全部变更与链式 SHA-256 摘要同事务落盘；
网页选择提交点即可核验截止该点的记录，缺行 / 改值 / 乱序都会指出封存起点
或最早失效事务，并把受影响的回溯结果标为不可信；启用前旧记录真实性标为未知。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：行变更 + 提交点 + 链式封存摘要在【同一个 SQLite 事务】落盘
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）：artifact_history + seals 哈希链
                                   ↕  FastAPI 网页 / JSON API（只读核验）
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更、提交点与其封存摘要在**同一个 SQLite 事务**落盘，不存在半条事务或“数据已写、封存缺失” |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type`、提交点与 `seal_seq`；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| **连续封存：每笔事务链式 SHA-256** | `seals` 只追加：seq=0 为 GENESIS（封存起点），其后每笔 TXN 的 `entry_hash=SHA256(规范化提交点+全部变更行)`，`chain_hash=SHA256(前驱 chain_hash ‖ entry_hash)`，任一历史行被改 / 缺失 / 乱序都会在复算时暴露 |
| **首次启用封存已有历史** | 旧版本库首次启动自动迁移（加 `seal_seq` 列、建 `seals` 表）并对当时全部历史补一条 GENESIS；它只能证明“启用后未变”，这些旧记录核验时标 `UNKNOWN`（启用前真实性未知），状态 `legacy_upgrade=true` |
| **网页核验指出最早失效事务** | `/api/verify?point=` 用同一历史视图复算截止该点的封存链：改值→`ENTRY_HASH_MISMATCH`、缺行→`ROW_COUNT_MISMATCH`、删整笔封存→`SEQ_GAP`、提交点乱序→`ORDER_MISMATCH`、重封/替换→`CHAIN_BROKEN`；返回 `first_invalid_seq`，其后回溯行标 `UNTRUSTED`，其前已独立验证的仍 `TRUSTED` |
| **核验时捕获继续** | 核验全程只读（独立短查询 + 内存复算），不持长锁、不写库；捕获线程照常落盘，互不阻塞 |
| **落盘失败不确认复制槽** | 数据/封存/槽位推进位置同事务，任一失败整体回滚；捕获线程仅在 commit 成功后 `send_feedback`，keepalive 与无关消息一律不回送反馈 |
| 强杀重启 / 重放不漏封、不重封 | 复制槽持久化在源库；重启从槽位重放，SQLite 以源事务 xid 为主键幂等去重——已落盘事务直接跳过，不会重复写入也不会重复封存；新事务从 `MAX(seq)+1` 续链，无空洞 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时历史与核验接口一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/消息 LSN、封存启用信息、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN、数值化 LSN 与 `seal_seq`（所属封存序号；旧库迁移行归入 GENESIS）。
- `seals`：只追加的封存哈希链。`seq`（0=GENESIS）、`kind`、`xid`、提交点、
  `entry_hash`（覆盖提交点+全部变更行）、`chain_hash`（链式衔接）、`row_count`、封存时间。

### 可信标记语义

| 标记 | 含义 |
| --- | --- |
| `TRUSTED` | 该记录被某条封存覆盖，且截止查询点该封存及其前驱全部复算通过 |
| `UNTRUSTED` | 封存链在 `first_invalid_seq` 处失效，该记录属于失效点或其后的封存 |
| `UNKNOWN` | 记录早于封存启用（旧库首次启用时 GENESIS 覆盖的旧行，或无封存档），启用前真实性无法由链证明——**不代表被篡改，只代表无法证明** |

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、**封存链状态（封存数 / 链是否完好 / 封存起点 LSN / 是否旧库升级）**、
   最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一失败整组回滚。
3. **提交点回看与核验**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数、封存序号）；每行有两个操作——
   - **回看目录**：查看该时刻整张目录（保留原回溯功能）；
   - **核验到此点**：以同一历史视图复算截止该点的链式 SHA-256，弹出核验结论
     （通过 / 失效原因与最早失效封存 seq、封存起点），并给每条回溯结果打
     “可信 / 不可信 / 真实性未知”徽章。
4. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链、每行封存序号
   与可信标记，直观验证“删除后重用编号，旧历史保留”及旧记录的可信边界。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（含轻量封存链 `seal`，初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（含 `seal_seq`，未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录（原回溯视图） |
| GET | `/api/verify?point=0/16B3748` | **封存核验**：不传 point 核验到最新点；返回 `status`、`first_invalid_seq/reason/detail`、`seal_genesis`、带 `trust` 的回溯行 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链，每行带 `trust` |

`/api/verify` 失效时 `status="INVALID"`，并给出 `first_invalid_reason`
（`ENTRY_HASH_MISMATCH` 改值 / `ROW_COUNT_MISMATCH` 缺行 / `SEQ_GAP` 删整笔封存 /
`ORDER_MISMATCH` 乱序 / `CHAIN_BROKEN` 重封替换）与人类可读 `first_invalid_detail`，
失效点及之后的回溯行 `trust="UNTRUSTED"`，其前已独立验证的仍 `TRUSTED`。

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
curl 'localhost:8000/api/verify?point=0/16B3748'
```

## 5. 自测

### 5.1 Python 单元测试（纯标准库，无需 PostgreSQL）

```bash
python3 -m unittest tests.test_decoder tests.test_seal                      tests.test_storage_seal tests.test_http_verify -v
```

- `test_decoder`：BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略；
- `test_seal`：entry_hash 确定性与改值/缺行/乱序敏感性、chain_hash 衔接、
  全链与截止点核验、最早失效点定位、可信标记；
- `test_storage_seal`：新装库 GENESIS+链式封存、旧库升级（旧行 UNKNOWN）、
  改值/缺行/删封存/乱序/断链检测、重放幂等不重封、重开后续链、落盘失败整体回滚；
- `test_http_verify`：在真实 FastAPI 路由上验证 `/api/verify`、`/api/points`、
  `/api/history` 的序列化、可信标记与参数校验。

### 5.2 端到端自测（61 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. SQLite 与 PostgreSQL 数据目录均落盘持久化；
9. **连续封存与网页核验**：每笔提交点带封存序号、链连续延伸且链接完好；
   强杀重启后封存不重复、核验仍通过；改值→`ENTRY_HASH_MISMATCH`、
   缺行→`ROW_COUNT_MISMATCH`、删封存→`SEQ_GAP` 均定位最早失效事务，
   受影响回溯标不可信、其前仍可信，精确还原后链恢复；核验期间捕获不中断；
10. **旧库首次启用封存**：自动迁移补 GENESIS（`legacy_upgrade=true`），
    启用前旧记录 `UNKNOWN`，启用后新事务 `TRUSTED`。

最近一次结果（本机便携 PostgreSQL 16.6）：**61 passed / 0 failed**。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 封存哈希链能检测对 SQLite 历史库的**任何事后改动**（缺行/改值/乱序/重封），
  但不阻止拥有库文件写权限的人删除整个库或同时重算整条链——它是“事后可核验
  的篡改证据”，不是访问控制；需要防删除/防重算时应把 `seals` 摘要外发
  （如定期只追加到远端/WORM 存储），本版预留了链结构与只读核验入口。
- 旧库首次启用时 GENESIS 只证明“启用后未变”，启用前的真实性无法自证，
  统一标 `UNKNOWN`，这是密码学封存的固有边界而非缺陷。
- 本沙箱无 Docker 守护进程，Compose 文件未经本机 `docker compose` 实跑；
  应用本体已用同等配置在本机便携 PostgreSQL 16.6 + uvicorn 下完成全部
  端到端验证（61 项断言），Compose 镜像基于官方 `python:3.12-slim`
  与 `postgres:16-bookworm`，新增的 `app/seal.py` 位于被 COPY 的 `app/` 目录内，
  无额外系统依赖（仅用 Python 标准库 `hashlib`/`json`）。
