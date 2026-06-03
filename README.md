# dataflow — 统一数据处理框架

一套五段式数据处理框架：**接入 → 识别 → 处理 → 判定 → 派发**。
框架负责编排、调度、容错、追踪、配置；各业务（楼宇 / 剧场 / 律师 …）
只写插件。实时信号流和文档/知识批处理都能装进同一套骨架。

```
Ingest → Classify → Process → Evaluate → Dispatch
接入      识别类型    简单处理   阈值/判定   派发任务
```

## 目录

```
core/        框架核心：DataEnvelope、插件接口、Registry、基础设施端口
rules/       配置驱动的规则引擎（expr 表达式 + JSON 规则），含 Evaluator 适配
pipeline/    Pipeline 编排器：状态机、容错重试、transfer.complete 延迟派发
plugins/
  temperature/  楼宇示例：温度信号，即时触发，超阈值告警
  eeg/          剧场示例：脑电信号，毫秒时间戳，持续唤醒峰值 → 关联分析
  legal/        律师示例：大文档引用，传输完成后解析/切分/向量化 → 写 RAG
examples/    把三个 vertical 接到同一条 pipeline 的可运行 demo
```

## 运行

```bash
go test ./...          # 单元测试
go run ./examples      # 三个 vertical 全链路演示
```

## 核心概念

**DataEnvelope** — 框架唯一的工作单元。所有数据归一成这个信封；
小数据走 `Payload`（内联字节），大对象走 `Ref`（对象存储 URI 引用），
所以同一信封既能承载几字节的温度读数，也能引用几 GB 的案件 PDF。

**五个插件接口**（`core/plugin.go`）：

| 接口 | 职责 | 你实现什么 |
|---|---|---|
| `Classifier` | 这条数据是什么类型 | Match + 打类型标签、提结构化字段 |
| `Processor` | 简单处理 | 清洗 / 单位换算 / 降采样 / 解析切分向量化 |
| `Evaluator` | 是否触发、派什么任务 | 通常直接用 `rules.RuleEvaluator` |
| `Dispatcher` | 执行任务 | 告警 / 抓取 / 写 RAG / 派发分析 |

**两种触发模式**：

- `Immediate` — 自包含数据（温度、EEG），评估命中即派发。
- `OnTransferDone` — 大对象（视频、大 PDF）。框架在处理后发现对象未传完
  且该类型有延迟规则，会把整条数据 park 起来，等 `transfer.complete`
  事件再重跑 Process（解析/切分/向量化）+ Evaluate + Dispatch。
  解决了"门槛规则需要解析后的对象、但对象还没传完"的鸡生蛋问题。

**规则配置化**（`examples/rules.json`）——阈值、条件、任务参数全在配置里，
交付后甲方可改，不动代码。`condition` 是 expr 表达式，对信封 Meta
扁平求值，并注入 `threshold`、`data_type`、`transfer_complete` 等内置变量。

```json
{
  "id": "temp_high",
  "data_type": "temperature",
  "condition": "value > threshold",
  "threshold": 60,
  "action": "alert",
  "need_task": true,
  "trigger_mode": "immediate",
  "task": { "type": "alert", "priority": "high" }
}
```

## 加一个新 vertical 要做什么

1. 实现 `Classifier`（识别你的数据）+ 一个或多个 `Processor`（处理）
   + `Dispatcher`（执行任务）。
2. 在 `rules.json` 里加该 `data_type` 的规则。
3. 启动时 `reg.Register*` 注册插件，复用 `rules.NewEvaluator(engine)`。

框架的状态流转、追踪（TraceID）、重试、延迟派发、审计日志全部免费获得。

## 基础设施端口（air-gap / 可替换）

框架只依赖 `core` 里的抽象接口，不绑定具体 DB/队列：

- `Store` — 落库 + 状态流转（生产可接 TimescaleDB + Postgres）
- `AlertSink` — 告警（收敛/去重在这层做）
- `EventBus` — 发布订阅（生产接 NATS / Kafka / MQTT），用于 `transfer.complete`、
  service 上下线事件
- `Logger` — 结构化日志

demo 里都是内存桩实现，换成真实现不影响上层。

## 已知的工程注意点（生产前补齐）

- **告警收敛**：同一设备同类预警在时间窗内去重，避免预警风暴。
  接在 `AlertSink` 实现里。
- **背压**：Dispatcher 慢时的限流；`EventBus` 要扛积压。
- **超时清理**：`Pipeline.SweepExpired` 需要用 ticker 定期调用，
  清理传输超时未完成的 park 项。
- **审计不可篡改**：`Store` 的状态流转日志在政企/等保场景要持久化且防篡改。
- **法律 RAG**：embedding 必须本地模型（数据不出域）；chunk 带版本/生效日期/
  效力状态和完整溯源元数据。
