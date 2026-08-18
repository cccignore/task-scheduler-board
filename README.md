# 任务调度看板

一个可本地运行的 FastAPI + SQLite 小型调度器：后端提供任务/组/Step API 与实时看板；每次数据库操作使用独立连接。**完整的设计思路、题目对照与取舍见 [DESIGN.md](DESIGN.md)。**参数解析在任务进入 `running` 时冻结组参数快照，然后按 `base → group snapshot → 有序 step override` 做浅层 key 合并；L3 中精确等于 `""` 的值不写入当前值，其余值（含 `0`、`false`、`null` 和新 key）会粘性传给后续 Step（`""` 语义只作用于顶层 key，嵌套对象整体替换、其内部的 `""` 是字面值）。认领成功时由数据库事务绑定一个服务端随机 token，后续 start/complete 必须同时提供 Worker ID 与 token；任务列表不暴露 token。

认领同时授予**租约**（默认 900s，`TASKBOARD_LEASE_SECONDS` 可调）：start 与每次成功上报都会续租；worker 崩溃后租约过期，下一次 claim-next 会在同一写事务里把任务回收回 pending 并轮换 token——旧 worker 的迟到请求全部 409，新 worker 从第一个未完成 Step 续跑，已完成 Step 的日志与首次冻结的 L2 快照保持不变。因此 worker 必须把 claim token 持久化，否则崩溃重启后只能等租约回收。写锁争用超过 busy_timeout 时 API 返回 503 + Retry-After，而非 500。

实际开发耗时：本次 Codex 辅助实现、对抗审阅与自动验证约 1.5 小时；后续由 Claude 辅助迭代租约/看板约 1 小时。实际为单人队伍，人工复核耗时未单独计时，完整披露见 `COLLAB.md`。

## 为什么是 Python，正确性如何保证

Python/FastAPI 能用少量代码把事务边界、API 和看板串起来，`sqlite3` 又允许测试直接创建真正独立的数据库连接。需要强调：并发证明使用 `multiprocessing` 的 **spawn** 模式启动独立 OS 进程，不用线程、`asyncio.gather` 或共享连接。`scripts/run_concurrency_proof.py` 让多个常驻进程逐轮同时争抢同一个 pending task；`scripts/run_idempotency_proof.py` 让 5 个进程同时上报同一个 Step，并在成功后追加失败重试。两者失败时均非零退出，实际输出及浏览器验收记录保存在 `evidence/`。

SQLite 认领在短事务中取得写锁，再以 `status = 'pending'` 为条件更新；锁直到提交才释放，所以其他连接不可能同时拿到同一任务（认领结果的详情读取放在写事务提交之后，缩短写锁持有时间）。日志以数据库唯一约束 `(task_id, step_sequence)` 作为最终幂等边界，冲突时保留首条记录，尤其不会让后到的失败覆盖成功；矛盾的重复上报会记入服务端日志便于排查。当前 SQLite/WAL 版本只面向同机进程共享一个数据库文件，不声称支持多台机器各持本地副本；跨机器部署应让所有 Worker 连接同一个 PostgreSQL/MySQL 服务。迁移时**不能照搬 SQLite 的 `BEGIN IMMEDIATE` 语法**，会改用 `SELECT … FOR UPDATE SKIP LOCKED` 后同事务更新（或等价的带状态条件原子更新），仍由行锁/条件更新保证唯一持有，由同一唯一约束保证日志幂等，核心不变量不变。

## 启动、验证、边界

要求 Python 3.9+：

```bash
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install '.[dev]'
uvicorn app.main:app --reload                    # http://127.0.0.1:8000
python3 -m pytest                                # 全部单元/API/租约/台账/多进程测试
python3 scripts/run_concurrency_proof.py         # 真实并发认领证据（页面也可一键运行）
python3 scripts/run_idempotency_proof.py         # 5 进程幂等证据（页面也可一键运行）
python3 scripts/run_worker.py --workers 3        # 命令行 worker（页面「模拟工人」同一段代码）
```

打开 `http://127.0.0.1:8000` 后**全部演示都可在页面完成，无需命令行**：五列流水线看板 + 首屏白话解释三个不变量；操作台三步走——造任务 → 一键上岗真实子进程的模拟工人（可选失败率）→ 或手动认领/启动/上报；点任务卡看 L1/L2/L3 配方三层与执行日志台账；「并发完成 ×5」现场验证幂等；「多进程证明」按钮在临时库上实跑 spawn 进程证明；「系统操作台账」实时展示所有进程写入 `operation_logs` 的认领/上报/重复忽略/矛盾告警/租约回收记录；「清空看板」一键重置。

测试主动覆盖：无组/空字典；L2 空字符串按字面覆盖；L2/L3 引入新 key；L3 空字符串回退当前粘性值；嵌套对象中的 `""` 是字面值；随机 override 链与独立 oracle 对照；连续覆盖；`0/false/null` 不被误判为空；组在 start 前修改可见、start 后修改不影响快照；非法/越序状态流转；成功日志后的失败重试；租约过期回收、token 轮换、断点续跑；操作台账全生命周期；托管 worker 真进程执行；证明与重置 API；API 和看板 smoke。

Step 1 到 Step N 的当前生效值如何逐 key 演变，见 [PARAMETER_EVOLUTION.md](PARAMETER_EVOLUTION.md)。

刻意删减的是生产级鉴权、分页与 schema migration（仅保留一次性的加列迁移）、`/api/demo` 为演示直接返回 token 的替代方案；题设规模下不引入 Redis/消息队列。删减项不影响参数、认领、租约回收和幂等日志这些核心不变量。

协作及 AI 使用披露见 [COLLAB.md](COLLAB.md)。
