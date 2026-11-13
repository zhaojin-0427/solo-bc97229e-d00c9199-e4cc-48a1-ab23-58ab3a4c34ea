# 环境检测数仓 · 批次导入 / 撤销 / 增量同步接入

Flask + SQLite 实现的单容器 Web 应用：保存仪器原始 CSV 批次，支持整批撤销（后台重建 +
原子切换），并为监管报送库维护人员提供**增量同步接入**——后台固定同一时刻的完整正式
结果快照与起始游标，就绪后分页读取初始数据，再按游标只拉后续变更，避免每天重拉全库。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

数据持久化在命名卷 `warehouse_data`（容器内 `/data`）。停止后重新 `up` 会自动接续
未完成的撤销任务；同步接入的就绪快照、游标与变更组同样保留，未完成接入标记失败且不供数。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
DATA_DIR=./data python -m gunicorn --workers=1 --threads=4 --bind=0.0.0.0:8000 wsgi:app
```

> 必须单 worker 多线程：撤销与快照构建的后台线程、"同时只允许一项撤销"约束依赖单进程；
> SQLite WAL 下读不阻塞写、写事务串行排队。

演示数据与脚本：

```bash
python tests/make_demo_data.py     # 生成 demo_data/*.csv
bash demo.sh                       # 命令行全流程演示（含同步接入分页与变更拉取）
```

## 数据模型（SQLite，`/data/warehouse.db`）

| 表 | 作用 |
|---|---|
| `batches` | 批次元数据，状态机 `active → revoking → revoked`（revoked 永久，无恢复入口） |
| `raw_records` | 原始 CSV 记录，只增不删；撤销后仍保留可审计，但查询时被过滤 |
| `current_results` | 正式结果表，对外只读消费；导入时 upsert，撤销时整体重建 |
| `revoke_jobs` | 撤销任务与进度，支持重启接续；部分唯一索引保证同时只有一项 |
| `change_groups` | 递增编号变更组（AUTOINCREMENT 单调递增，回滚不重用），只增不改 |
| `change_entries` | 组内条目：`upsert` 存完整新记录，`delete` 只存键（值为 NULL） |
| `sync_accesses` | 同步接入状态与起始游标；完整快照存于独立影子文件 `sync_snapshot_<id>.db` |

## 关键语义与实现方式

### 1. 导入
- CSV 四列：站点、采样日、检测项目、结果（兼容英文表头、UTF-8 BOM、GBK）。
- 批内前三列组合唯一，重复或格式错误**整批拒绝、事务回滚，不产生任何变更组**。
- 成功后分配自增批次号；`current_results` 按 `(站点,采样日,检测项目)` upsert，
  且 `WHERE excluded.source_batch > current_results.source_batch`，迟到/乱序写入
  永远不会覆盖更新批次的值。

### 2. 变更组（与正式结果同一事务提交）
每次导入或撤销，只把**真正变化的键**记成一个递增编号的变更组：

- **upsert**：键为新增，或 `result` / `source_batch` 与旧值不同 -> 携带完整新记录；
- **delete**：撤销后该键再无任何有效批次 -> 只返回键，不返回值；
- 值与来源批次都没变的记录、以及失败回滚的操作，**不产生任何变更**；
- 变更组与正式结果在同一个写事务内提交，要么同时可见、要么同时不可见；
- 已发布的组只增不改，可重复读取。

### 3. 撤销：影子重建 + 单事务原子切换
1. 请求撤销：批次置 `revoking`，登记任务并记录**快照批次号**（当时最大批次号）。
2. 后台线程在**独立的影子 SQLite 文件**中按键重算"最大未撤销批次"的值，
   分页推进并把进度写回正式库（仅秒级短事务），**重建不占用正式库写事务**。
3. 切换：单个 `BEGIN IMMEDIATE` 事务内先把旧正式结果存入临时表，ATTACH 影子库 →
   清空重写 `current_results` → 合并快照之后导入的批次 → 批次置 `revoked` →
   与新旧状态逐键比对生成撤销变更组，最后统一提交。
4. 失败：任务置 `failed`、批次恢复 `active`、事务回滚，撤销与变更组都不生效，可重新发起。
5. 重启：启动时自动发现 `pending/running` 任务，影子表支持 keyset 断点续建。
6. 同时只允许一项：应用层检查 + 数据库部分唯一索引双重保证。

### 4. 同步接入：固定时刻快照 + 游标增量

```
POST /api/sync/accesses                创建接入（后台构建快照，可带 page_size）
GET  /api/sync/accesses                接入列表与状态
GET  /api/sync/accesses/<id>           单个接入状态（ready 后返回起始游标 cursor）
GET  /api/sync/accesses/<id>/initial   分页读初始数据（?cursor=&limit=）
GET  /api/sync/accesses/<id>/changes   按游标拉取下一完整变更组（?cursor=）
```

- **固定时刻、不占写事务、内存恒定**：后台线程在一个只读长事务里冻结正式库某一时刻的
  一致快照（WAL 下读不阻塞写），用 keyset 分页把完整正式结果写入**独立影子 SQLite 文件**，
  每次只持有一页数据，内存不随结果行数增长；构建期间上传、撤销、取数全程可用。
- **起始游标与衔接保证**：快照事务同时冻结"快照时刻最大变更组 G"，初始数据恰好覆盖
  第 1..G 组的全部效果；变更游标从 G 开始，只返回 `group_id > G` 的组——
  初始数据与后续组之间**不漏更、不重复跨界**。
- **分页读取初始数据**：每页返回 `records` 与 `next_cursor`；最后一页的
  `next_phase` 变为 `changes`，游标无缝切换为变更游标。
- **每次拉取一个完整组**：`/changes` 每次恰好返回游标的下一完整组及新游标，**绝不拆组**；
  没有新组时返回 `group: null` 和同一游标，可安全轮询；同一游标重复读取得到同一组。
- **游标**：不透明 base64url 字符串，内置 `access_id` 与类型（initial/changes），
  跨接入使用、类型混用或非法游标均被拒绝；游标可离线保存，重启后继续有效。
- **重启语义**：影子文件、游标与变更组都在磁盘上。启动时 `ready` 接入原样保留供数；
  只有停在 `building` 的接入会被标记 `failed`、清理影子文件且**不供数**，重新创建即可。

### 5. 页面
`GET /` 单页应用包含五个区：① 上传 CSV 批次；② 批次列表与撤销；③ 撤销任务进度；
④ 当前正式结果与 CSV 下载；⑤ 同步接入——创建接入、查看构建状态、翻页读初始数据、
粘贴/沿用游标拉取变更组，并展示当前游标与原始 JSON 响应。原有批次管理功能保持不变。

## 测试

```bash
python -m unittest tests.test_app tests.test_sync -v
```

- `tests/test_app.py`（11 个）：递增批次号与同键最新值、批内重复整批拒绝、撤销重算与
  孤儿键删除、重建期间并发导入合并、并发撤销互斥、注入故障的失败回滚、重启接续、CSV 下载。
- `tests/test_sync.py`（12 个）：快照构建与分页、快照一致性、导入/撤销变更组精确内容、
  失败操作不产变更、整组拉取不拆组与可重复读、初始/变更衔接不漏不重的重放不变量、
  构建期间服务可用性、游标校验、重启后 ready 保留与 building 失败不供数。

## 调参（环境变量）

- `REBUILD_PAGE_SIZE`：撤销影子重建每批键数（默认 10，演示调小可观察进度条）
- `REBUILD_SLEEP`：撤销重建每页间隔秒数（默认 0.15）
- `SYNC_PAGE_SIZE`：同步快照默认每页行数（默认 100）
- `SYNC_MAX_PAGE_SIZE`：取数 `limit` 与创建接入 `page_size` 的上限（默认 1000）
