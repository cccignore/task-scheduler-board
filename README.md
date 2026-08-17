# 任务调度看板

一个可本地运行的 FastAPI + SQLite 小型调度器：后端提供任务/组/Step API 与静态看板；每次数据库操作使用独立连接。参数解析在任务进入 `running` 时冻结组参数快照，然后按 `base → group snapshot → 有序 step override` 做浅层 key 合并；L3 中精确等于 `""` 的值不写入当前值，其余值（含 `0`、`false`、`null` 和新 key）会粘性传给后续 Step。认领成功时由数据库事务绑定一个服务端随机 token，后续 start/complete 必须同时提供 Worker ID 与 token；任务列表不暴露 token。

实际开发耗时：本次 Codex 辅助实现、对抗审阅与自动验证约 1 小时。实际为单人队伍，没有第二名人工成员；人工复核耗时未单独计时，完整披露见 `COLLAB.md`。

## 为什么是 Python，正确性如何保证

Python/FastAPI 能用少量代码把事务边界、API 和看板串起来，`sqlite3` 又允许测试直接创建真正独立的数据库连接。需要强调：并发证明使用 `multiprocessing` 的 **spawn** 模式启动独立 OS 进程，不用线程、`asyncio.gather` 或共享连接。`scripts/run_concurrency_proof.py` 让多个常驻进程逐轮同时争抢同一个 pending task；`scripts/run_idempotency_proof.py` 让 5 个进程同时上报同一个 Step，并在成功后追加失败重试。两者失败时均非零退出，实际输出保存在 `evidence/`。

SQLite 认领在短事务中取得写锁，再以 `status = 'pending'` 为条件更新；锁直到提交才释放，所以其他连接不可能同时拿到同一任务。日志以数据库唯一约束 `(task_id, step_sequence)` 作为最终幂等边界，冲突时保留首条记录，尤其不会让后到的失败覆盖成功。迁移到 Postgres/MySQL 时，**不能照搬 SQLite 的 `BEGIN IMMEDIATE` 语法**；会把认领事务替换为 `SELECT … FOR UPDATE SKIP LOCKED` 后同事务更新（或等价的带状态条件原子更新），仍由行锁/条件更新保证唯一持有，由同一唯一约束保证日志幂等，核心不变量不变。

## 启动、验证、边界

要求 Python 3.9+：

```bash
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install '.[dev]'
uvicorn app.main:app --reload                    # http://127.0.0.1:8000
python3 -m pytest                                # 全部单元/API/多进程测试
python3 scripts/run_concurrency_proof.py         # 真实并发认领证据
python3 scripts/run_idempotency_proof.py         # 5 进程幂等证据
```

打开 `http://127.0.0.1:8000`，点击“一键创建演示任务”，即可在 running Step 上执行“并发完成 ×5”。

测试主动覆盖：无组/空字典；L2 空字符串按字面覆盖；L2/L3 引入新 key；L3 空字符串回退当前粘性值；连续覆盖；`0/false/null` 不被误判为空；嵌套值整体替换；组在 start 前修改可见、start 后修改不影响快照；非法/越序状态流转；成功日志后的失败重试；API 和看板 smoke。

刻意删减的是生产级鉴权、任务租约/崩溃重领、自动 worker 执行器、分页与 schema migration；题设规模下不引入 Redis/消息队列。当前演示聚焦题目要求的参数、认领、幂等日志和状态看板，删减项不影响这些不变量。

协作及 AI 使用披露见 [COLLAB.md](COLLAB.md)。
