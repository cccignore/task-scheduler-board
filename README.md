# 任务调度看板

FastAPI + SQLite(WAL) 的小型任务调度器：多 worker 进程并发认领多 Step 任务、幂等上报，配一个可完整操作与验证的实时看板。**设计思路、题目逐条对照与取舍详见 [DESIGN.md](DESIGN.md)**；参数演变推演见 [PARAMETER_EVOLUTION.md](PARAMETER_EVOLUTION.md)；协作与 AI 披露见 [COLLAB.md](COLLAB.md)。

实际耗时：AI 辅助开发累计约 1.5 小时（三轮，明细见 COLLAB.md）；人工投入主要在需求确认、方向决策与逐轮验收，未单独计时。

核心机制：参数在任务首次 start 的事务内冻结 L2 组快照，按 `base → 组快照 → 有序 L3 粘性覆盖` 解析（仅 L3 顶层的精确 `""` 表示保持当前值）；认领用 `BEGIN IMMEDIATE` 写锁 + 条件更新保证唯一持有，并绑定服务端签发的 claim token；执行日志以唯一约束 `(task_id, step_sequence)` 兜底，首写生效、矛盾上报仅在操作台账留痕。认领附带**开工租约**：只回收"认领后一直未开工"的任务（此时无任何外部副作用，回收安全）；**已开工任务绝不自动重派**——凭证轮换挡得住数据库写入、挡不住已发出的消息，卡死任务由操作员在页面上手动重派（凭证作废、从首个未完成 Step 续跑）。

## 为什么是 Python，并发测试如何保真

Python/FastAPI 用最少代码串起事务、API 与看板；`sqlite3` 允许测试创建真正独立的数据库连接。并发证明使用 `multiprocessing` **spawn** 真实进程（非线程、非 asyncio 伪并发），每进程自验证独立连接，屏障同步后同一瞬间抢同一任务，赛后核对数据库终态与凭证唯一性。SQLite 版仅限同机共享数据库文件；迁移 PG/MySQL 时改用 `SELECT … FOR UPDATE SKIP LOCKED` + 条件更新，唯一持有与幂等的不变量不依赖 SQLite 特性（详见 DESIGN.md §4.5）。

## 启动与验证（Python 3.9+）

```bash
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install '.[dev]'
uvicorn app.main:app --reload        # http://127.0.0.1:8000
python3 -m pytest                    # 23 项测试（含真实多进程证明）
```

打开页面即可完成全部演示，无需命令行：造任务 → 一键上岗真实子进程模拟工人 → 看板流动；点任务卡看配方三层与执行日志；「并发完成 ×5」验证幂等（1 写入 + 4 no-op）；「多进程证明」现场实跑 spawn 进程（终端等价命令：`scripts/run_concurrency_proof.py`、`run_idempotency_proof.py`、`run_worker.py`）；「系统操作台账」展示所有进程共写的认领/上报/重复忽略/回收记录；支持手动重派与一键清空。实跑证据在 `evidence/`。

## 边界与刻意取舍

无鉴权、无分页、无正式 migration（仅一次性加列）、不引入消息队列/Redis（题设规模下数据库事务足够）；worker 需自行持久化 claim token 才能跨自身重启续报；写锁争用超时返回 503 + Retry-After。完整清单与理由见 DESIGN.md §7、§9。
