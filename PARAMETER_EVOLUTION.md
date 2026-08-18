# 当前生效值如何从 Step 1 演变到 Step N

这份说明回答一个核心问题：任务运行到任意 Step 时，它看到的“当前生效值”究竟是怎么得到的？

## 先给出精确规则

记：

- `B` 是 Task 的 L1 基础参数；
- `G*` 是 Task **首次成功 start 时**读取并保存的 L2 Group 参数快照，无 Group 时是空对象 `{}`；
- `O_i` 是第 `i` 个 Step 自己的 L3 override；
- `E_i` 是第 `i` 个 Step 实际使用、并保存到数据库的参数快照。

先计算 Step 之前的初始当前值：

```text
E_0 = shallow_merge(B, G*)
```

也就是先复制 `B`，再用 `G*` 中的同名顶层 key 直接覆盖它。L2 中的空字符串 `""` 也是一个普通字面值，会真正覆盖 L1。

然后按 Step 顺序递推：

```text
E_i = independent_copy(E_(i-1))

for each (key, value) in O_i:
    if value is exactly the JSON string "":
        do nothing                 # 保持当前值；原先不存在就继续不存在
    else:
        E_i[key] = copy(value)     # 当前 Step 覆盖，并粘性传给后续 Step
```

因此，只有 L3 的“精确 JSON 空字符串”有保持语义。`0`、`false`、`null`、空对象 `{}` 和空数组 `[]` 都不是这个特殊值，会正常写入。

### “浅合并”与“独立快照”不矛盾

合并语义是**浅层的**：只比较顶层 key，遇到嵌套对象就把整个对象当作一个值替换，不递归补齐其内部字段。但保存每个 `E_i` 时会做独立复制，所以修改后一个快照不会反向污染前一个快照。

由此推论：**`""` 的保持语义只作用于顶层 key**。嵌套对象内部出现的 `""`（如 `{"cfg": {"a": ""}}`）只是普通数据——整个 `cfg` 对象作为一个值粘性覆盖，内部的空串原样保留，不触发“保持当前值”。

### 崩溃恢复不改变参数语义

任务被回收（未开工的租约自动回收，或已开工任务被操作员手动重派）后由新 Worker 续跑时，`G*` 仍是**首次** start 冻结的快照（存储在任务行里，resume 时直接复用，不会重读已变化的组），所有 `E_i` 因输入不变而保持相同。`test_manual_requeue_rotates_credentials_and_resumes_from_pending_step` 对此有断言。

## 一个完整的 Step 1 → Step 5 演算

下面 L1、L2 和五个 L3 的数据取自 `test_complete_parameter_merge_boundary_matrix`。再把 `test_group_override_is_snapshotted_exactly_at_start` 验证的启动时序放到同一条时间线上：

1. Task 创建后、start 之前，Group 仍可修改；这些修改对本 Task 可见。
2. 首次成功 start 在事务中把当时的 Group 值冻结成 `G*`，并一次性算出所有 `E_1 ... E_5`。
3. start 之后再改 Group，已保存的 `G*` 和 `E_i` 不变；同一持有者重试 start 也直接复用已有快照。

本例 start 瞬间冻结的 L1/L2 输入如下。`--` 表示该层没有这个 key；`""` 表示 key 存在，值是空字符串。

| key | L1 `B` | L2 启动快照 `G*` | 初始当前值 `E_0` |
| --- | --- | --- | --- |
| `sender` | `"base"` | `"group"` | `"group"` |
| `base_only` | `"B"` | -- | `"B"` |
| `blank` | -- | `""` | `""` |
| `group_only` | -- | `"G"` | `"G"` |
| `count` | `7` | `0` | `0` |
| `flag` | `true` | `false` | `false` |
| `nullable` | `"from-base"` | `null` | `null` |
| `nested` | `{"layer":"base","discarded":true}` | `{"layer":"group"}` | `{"layer":"group"}` |
| `new_key` | -- | -- | -- |
| `never_seen` | -- | -- | -- |

这里已经能看出两个边界：L2 可以新增 `blank` / `group_only`，而 `blank: ""` 必须保留；`nested` 则整体替换，所以 L1 中的 `discarded` 不会被递归合并进来。

五个 Step 的 L3 输入与触发的规则是：

| Step | L3 override `O_i` | 本步的关键意义 |
| --- | --- | --- |
| 1 `first` | `sender="step-1"`, `new_key="new-1"`, `count=0`, `flag=false`, `nullable=null` | 普通覆盖、L3 新 key，以及非空字符串的 JSON 假值都正常写入 |
| 2 `empty-means-keep-current` | `sender=""`, `new_key=""`, `group_only=""`, `base_only=""` | 空字符串不清空，而是保持各 key 的当前值 |
| 3 `later-overrides-win` | `sender="step-3"`, `new_key="new-3"`, `count=5`, `nested={"layer":"step"}` | 后续非空值再次覆盖，并成为新的粘性当前值；嵌套对象整体替换 |
| 4 `sticky-with-no-override` | `{}` | 没有覆盖，所有当前值原样传递 |
| 5 `empty-new-key-does-not-create-it` | `sender=""`, `never_seen=""` | `sender` 保持 Step 3 的值；从未存在的 `never_seen` 仍然不会被创建 |

递推后，每一列都是当个 Step 真正看到的完整当前值：

| key | `E_0` | Step 1 `E_1` | Step 2 `E_2` | Step 3 `E_3` | Step 4 `E_4` | Step 5 `E_5` |
| --- | --- | --- | --- | --- | --- | --- |
| `sender` | `"group"` | `"step-1"` | `"step-1"` | `"step-3"` | `"step-3"` | `"step-3"` |
| `base_only` | `"B"` | `"B"` | `"B"` | `"B"` | `"B"` | `"B"` |
| `blank` | `""` | `""` | `""` | `""` | `""` | `""` |
| `group_only` | `"G"` | `"G"` | `"G"` | `"G"` | `"G"` | `"G"` |
| `new_key` | -- | `"new-1"` | `"new-1"` | `"new-3"` | `"new-3"` | `"new-3"` |
| `never_seen` | -- | -- | -- | -- | -- | -- |
| `count` | `0` | `0` | `0` | `5` | `5` | `5` |
| `flag` | `false` | `false` | `false` | `false` | `false` | `false` |
| `nullable` | `null` | `null` | `null` | `null` | `null` | `null` |
| `nested` | `{"layer":"group"}` | `{"layer":"group"}` | `{"layer":"group"}` | `{"layer":"step"}` | `{"layer":"step"}` | `{"layer":"step"}` |

从整体快照看，测试的核心断言等价于：

```text
[E_1, E_2, E_3, E_4, E_5] = [E_1, E_1, E_3, E_3, E_3]
```

这不是说 Step 1 和 Step 2 共享同一个可变对象，而是说两个独立快照的内容相等。

`0 / false / null` 在上表 Step 1 中确实作为 L3 输入被写入。另一个纯函数测试还故意让变化更显眼：从 `zero=9, flag=true, nullable="base"` 变为 `zero=0, flag=false, nullable=null`，以排除实现把它们误当成“空值”跳过的可能。

## 测试证据与边界对照

以下都是可执行断言，不只是文档中的口头约定：

| 测试 | 它特意证明什么 |
| --- | --- |
| [`test_pure_chain_returns_independent_snapshots_and_keeps_json_falsy_values`](tests/test_parameters.py) | L2 空字符串保留；L3 的 `0/false/null` 正常写入；L3 空字符串保持旧值且不创建新 key；修改后一快照不影响前一快照 |
| [`test_only_exact_empty_string_skips_and_nested_snapshots_are_deep_copies`](tests/test_parameters.py) | 只有精确 `""` 才跳过；`{}`、`[]`、`" "` 都正常覆盖并粘性传递；嵌套 list/dict 在不同 Step 快照间也是深度独立的 |
| [`test_complete_parameter_merge_boundary_matrix`](tests/test_parameters.py) | 逐 key 跑完上述 Step 1–5，覆盖 L1/L2/L3、L2/L3 新 key、L2/L3 空字符串差异、粘性传递、后续再覆盖、JSON 假值、嵌套对象整体替换，并断言完整快照列表与 `never_seen` 不存在 |
| [`test_empty_string_inside_nested_object_is_a_literal_value`](tests/test_parameters.py) | 嵌套对象整体替换；对象内部的 `""` 是字面值，不触发保持语义 |
| [`test_random_override_chains_match_the_reference_oracle`](tests/test_parameters.py) | 200 组随机 L1/L2/L3 链与一个独立实现的参照模型逐快照比对：“某 key 的当前值 = 截至本 Step 最后一次非空覆盖，否则初始合并值” |
| [`test_no_group_and_empty_dictionaries`](tests/test_parameters.py) | 没有 Group 时 `G*={}` 仍能正常递推；空 Step 字典和对旧 key 的空字符串都不会丢掉 L1；后续 Step 可新增 key |
| [`test_group_override_is_snapshotted_exactly_at_start`](tests/test_parameters.py) | Group 创建值不是永久冻结值：start 前更新可见，start 后更新不泄漏，持有者重试 start 仍复用已保存的 `G*` 和 `E_i` |

对应实现只有两个关键位置：[`resolve_parameter_chain`](app/services.py) 负责形式规则与独立快照；[`start_task`](app/services.py) 负责在事务中读取 `G*`、计算全部 `E_i` 并持久化。

运行证明：

```bash
python3 -m pytest tests/test_parameters.py -q
```
