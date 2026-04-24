# nanobot context_manager

上下文窗口裁剪扩展，通过 `AgentHook` 机制在每次工具调用后对返回内容进行软件层面的管理，减少发送给模型的 token 数量，同时保留模型自主导航获取完整内容的能力。

---

## 设计原则

- **侵入极小**：不修改 `runner.py` 和工具实现，仅通过 `AgentLoop` 已有的 `_extra_hooks` 机制注入
- **配置驱动**：默认关闭，通过 `nanobot.yaml` 一行开启
- **透明可导航**：截断内容末尾始终附有具体的工具调用示例，模型可自主获取剩余内容
- **Session 级状态**：`FileReadTracker` 在一次 `AgentRunner.run()` 内跨 iteration 追踪已读文件

---

## 文件结构

```
context_manager/
├── __init__.py   # 公开 ContextPrunerHook
├── hook.py       # ContextPrunerHook(AgentHook) — 拦截入口
├── pruners.py    # per-tool 裁剪函数
├── tracker.py    # FileReadTracker — 文件读取状态
└── README.md
```

---

## Hook 时序

```
_request_model
    ↓
append assistant_message to messages
    ↓
_execute_tools → raw results
    ↓
_normalize_tool_result → append tool messages
    ↓
hook.after_iteration(context)   ← ContextPrunerHook 在此介入
    ↓
next iteration
```

`after_iteration` 触发时，`context.messages` 已包含本次 iteration 新增的 tool 消息。`ContextPrunerHook` 通过 `tool_call_id` 匹配消息并原地修改 `content`，对下次模型调用生效。

---

## 裁剪策略

### read_file

| 情形 | 触发条件 | 处理方式 |
|------|---------|---------|
| **大文件** | 行数 > `max_file_lines`（默认 500） | 保留开头 `head_lines`（200）行 + 结尾 `tail_lines`（50）行，中间替换为省略标记，末尾附导航提示 |
| **重复读取** | 同一路径在本 session 内已读过 | 返回简短提醒 + `repeat_read_head_lines`（50）行预览，提示模型查看历史上下文 |
| **分页读取** | 调用已携带 `offset` 或 `limit` 参数 | 不干预，模型正在主动翻页 |

导航提示示例：
```
[Context Manager] … 1200 lines omitted …

[Context Manager] Content truncated.
  → read_file("path/to/file", offset=201, limit=200)  to continue reading
  → grep("<pattern>", "path/to/file")  to search specific content
```

### exec

| 触发条件 | 处理方式 |
|---------|---------|
| 输出行数 > `max_exec_lines`（默认 200） | 保留开头 `head_lines` 行 + 结尾 `tail_lines` 行，**始终保留 exit code 行**，附 head/tail/grep 用法提示 |

### grep

| 触发条件 | 处理方式 |
|---------|---------|
| 匹配行数 > 100 | 截断为前 100 行，附缩小搜索范围的建议 |

---

## 配置参数

在 `nanobot.yaml` 的 `agent.defaults` 或顶层 `agent` 节点下：

```yaml
agent:
  contextManager:
    enabled: true                # 必须显式开启，默认 false
    maxFileLines: 500            # read_file 截断阈值（行数）
    headLines: 200               # 截断时保留的开头行数（read_file & exec 共用）
    tailLines: 50                # 截断时保留的结尾行数（read_file & exec 共用）
    maxExecLines: 200            # exec 输出截断阈值（行数）
    repeatReadHeadLines: 50      # 重复读提醒中显示的预览行数
```

所有参数均有默认值，`enabled: true` 是唯一必须设置的字段。

---

## SKILL.md

`nanobot/skills/context-manager/SKILL.md` 是配套的内置 skill（`always: true`），在系统提示中向模型说明：

- 文件内容可能被截断，以及如何用 `offset`/`limit` 继续分页
- 重复读取文件时应优先查看历史上下文
- `exec` 长输出的过滤方式（`head`、`tail`、`grep`）
- `grep` 结果过多时如何缩小范围

该 skill 确保模型在收到 `[Context Manager]` 标记的内容时能正确理解并采取行动，而不是重复请求相同数据。

---

## 与框架的集成点

### 自动注入（推荐）

通过配置启用后，`AgentLoop.__init__` 会自动将 `ContextPrunerHook` 添加到 `_extra_hooks`：

```python
# nanobot/agent/loop.py（已添加）
if defaults.context_manager.enabled:
    from nanobot.context_manager import ContextPrunerHook
    self._extra_hooks = [ContextPrunerHook(defaults.context_manager), *self._extra_hooks]
```

### 手动注入

也可以在构造 `AgentLoop` 时直接传入：

```python
from nanobot.context_manager import ContextPrunerHook
from nanobot.config.schema import ContextManagerConfig

cfg = ContextManagerConfig(enabled=True, max_file_lines=300)
loop = AgentLoop(..., hooks=[ContextPrunerHook(cfg)])
```

---

## 与分析脚本的配合

`analysis/compression_analysis.py` 用于量化 context manager 的效果：

```bash
# baseline = 未启用 context manager 的跑
# test     = 启用后的跑
conda run -n nanobot python analysis/compression_analysis.py \
    --baseline skill/results/trajectories/0007 \
    --test     skill/results/trajectories/0009
```

输出每任务的 obs token 节省率、总 token 节省率以及评分变化，评估压缩是否影响精度。
