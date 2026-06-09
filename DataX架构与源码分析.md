# DataX 架构与源码深度分析

> 基于 Alibaba 开源 DataX 3.0 源码梳理，涵盖整体架构、工作流程、底层实现原理。

---

## 目录

1. [项目概览](#1-项目概览)
2. [模块结构](#2-模块结构)
3. [整体架构设计](#3-整体架构设计)
4. [启动与执行流程](#4-启动与执行流程)
5. [核心概念模型](#5-核心概念模型)
6. [配置系统](#6-配置系统)
7. [插件体系与加载机制](#7-插件体系与加载机制)
8. [数据模型与传输层](#8-数据模型与传输层)
9. [调度与并发控制](#9-调度与并发控制)
10. [统计监控与脏数据处理](#10-统计监控与脏数据处理)
11. [容错与 Failover](#11-容错与-failover)
12. [Transformer 数据转换](#12-transformer-数据转换)
13. [完整执行示例](#13-完整执行示例)
14. [关键源码索引](#14-关键源码索引)

---

## 1. 项目概览

DataX 是阿里巴巴开源的**异构数据源离线同步工具**，采用 **Framework + Plugin** 架构：

- **Reader**：数据采集插件，从源端读取数据
- **Writer**：数据写入插件，向目的端写入数据
- **Framework**：连接 Reader/Writer 的中间层，负责缓冲、流控、并发调度、类型转换、统计监控等

设计理念是将复杂的网状同步链路转化为**星型链路**——DataX 作为中心枢纽，新数据源只需开发对应插件即可接入。

技术栈：

| 项目 | 说明 |
|------|------|
| 语言 | Java 8 |
| 构建 | Maven 多模块 |
| JSON 配置 | Fastjson2 |
| 运行模式 | 开源版默认 Standalone（单机多线程） |

---

## 2. 模块结构

### 2.1 顶层模块划分

```
datax-all (根 POM)
├── common              # 公共基础：Configuration、SPI、Record/Column、插件抽象
├── transformer         # 数据转换器 API
├── core                # 框架核心：Engine、Container、Channel、统计
├── plugin-rdbms-util   # RDBMS 插件公共逻辑（切分、读写）
├── plugin-unstructured-storage-util  # 非结构化存储公共逻辑（HDFS/FTP/文本等）
├── {reader插件} × ~40  # mysqlreader、hdfsreader、gdbreader ...
├── {writer插件} × ~45  # mysqlwriter、hdfswriter、gdbwriter ...
└── datax-example       # IDE 本地调试示例
```

### 2.2 运行时目录结构（打包后）

```
{DATAX_HOME}/
├── bin/datax.py          # Python 启动脚本
├── conf/
│   ├── core.json         # 框架核心配置
│   └── logback.xml       # 日志配置
├── lib/
│   └── datax-core-*.jar  # 框架核心 JAR
├── plugin/
│   ├── reader/{name}/    # plugin.json + jar + libs/
│   └── writer/{name}/
├── job/                  # 示例 Job JSON
└── log/
```

环境变量 `datax.home`（由 `datax.py` 通过 `-Ddatax.home=...` 设置）是所有路径解析的基准。

---

## 3. 整体架构设计

### 3.1 逻辑分层

```
┌─────────────────────────────────────────────────────────────┐
│                      用户 Job JSON                           │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Engine（入口）                                              │
│  ConfigParser → LoadUtil → JobContainer / TaskGroupContainer │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  JobContainer（作业调度中枢）                                 │
│  init → prepare → split → schedule → post → destroy         │
│  加载 Reader.Job / Writer.Job 插件                           │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Scheduler（StandAloneScheduler）                            │
│  将 Task 分配到多个 TaskGroup，线程池并发执行                  │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  TaskGroupContainer（任务组执行器）                           │
│  管理 N 个 Channel 并发，每个 Channel 运行 1 个 Task           │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  TaskExecutor（最小执行单元）                                 │
│  ReaderRunner(thread) ←→ Channel ←→ WriterRunner(thread)    │
│  加载 Reader.Task / Writer.Task 插件                         │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 执行单元层次关系

```
Engine
 └── JobContainer                    # 1 个 Job 对应 1 个 JobContainer
      └── StandAloneScheduler
           └── TaskGroupContainer × M  # M 个任务组（线程池并发）
                └── TaskExecutor × N   # 每组最多 N 个并发 Task（默认 5）
                     ├── ReaderRunner (Thread)
                     ├── MemoryChannel
                     └── WriterRunner (Thread)
```

### 3.3 三种运行模式

| 模式 | 说明 | 开源版支持 |
|------|------|-----------|
| **Standalone** | 单进程，无外部依赖 | ✅ 默认 |
| **Local** | 单进程，统计上报集中存储 | 代码保留 |
| **Distributed** | 多进程，依赖 DataX Service | 代码保留 |

开源版 `JobContainer.schedule()` 中硬编码 `ExecuteMode.STANDALONE`，但插件代码与模式无关。

---

## 4. 启动与执行流程

### 4.1 启动链路

```bash
python {DATAX_HOME}/bin/datax.py {job.json}
```

`datax.py` 组装 Java 命令：

```
java -server -Xms1g -Xmx1g \
  -Ddatax.home={DATAX_HOME} \
  -classpath {DATAX_HOME}/lib/* \
  com.alibaba.datax.core.Engine \
  -mode standalone -jobid -1 -job {job.json}
```

### 4.2 Engine 启动序列

`Engine.entry()` → `Engine.start()` 核心步骤：

```
1. 解析命令行参数（-job, -jobid, -mode）
2. ConfigParser.parse(jobPath)     # 合并 job.json + core.json + plugin.json
3. MessageSource.init()            # 国际化
4. ConfigurationValidate.doValidate() # 配置校验（目前多为空实现）
5. ColumnCast.bind()               # 列类型/格式绑定
6. LoadUtil.bind()                 # 注册插件配置
7. 根据 core.container.model 选择容器：
   - 默认 → JobContainer
   - taskGroup → TaskGroupContainer（分布式子进程场景）
8. PerfTrace 初始化
9. container.start()
```

### 4.3 JobContainer 生命周期

```
preHandle → init → prepare → split → schedule → post → postHandle → destroy
```

| 阶段 | 职责 | 调用对象 |
|------|------|---------|
| **preHandle** | 前置处理插件（可选） | Handler 插件 |
| **init** | 初始化 Reader.Job、Writer.Job | 插件 Job 层 |
| **prepare** | 全局准备（如清空目标表） | 插件 Job 层 |
| **split** | 切分 Task，合并 Reader/Writer 配置 | 插件 Job 层 |
| **schedule** | 分配 TaskGroup，启动调度 | Scheduler |
| **post** | 全局后置（如 rename 影子表） | 插件 Job 层 |
| **postHandle** | 后置处理插件（可选） | Handler 插件 |
| **destroy** | 资源释放 | 插件 Job 层 |

### 4.4 Task 层生命周期

每个 Task 在 `TaskExecutor` 中启动两个线程：

```
WriterRunner: init → prepare → startWrite(RecordReceiver) → post → destroy → markSuccess()
ReaderRunner: init → prepare → startRead(RecordSender) → terminate() → post → destroy
```

**关键设计**：Writer 线程先启动并阻塞等待数据；Reader 完成后调用 `terminate()` 发送结束信号；**只有 WriterRunner 调用 `markSuccess()`**，避免 Reader 先结束导致 Writer 未完成的竞态问题。

---

## 5. 核心概念模型

### 5.1 Job

一次完整的数据同步作业，例如「MySQL 某表 → HDFS 某路径」。对应用户提交的 Job JSON，由 `JobContainer` 管理。

### 5.2 Task

Job 切分后的**最小执行单元**。例如 100 张分表切分为 100 个 Task，每个 Task 负责一部分数据的读写。Task 之间完全独立、可并发。

### 5.3 Channel

**1 个 Reader Task 与 1 个 Writer Task 之间的数据传输管道**。框架强制 1:1 模型——Reader 切分 N 个 Task，Writer 也必须切分 N 个 Task。

### 5.4 TaskGroup

一组 Task 的集合，由同一个 `TaskGroupContainer` 管理。默认每个 TaskGroup 最多并发 **5 个 Channel**（`core.container.taskGroup.channel`）。

### 5.5 调度决策示例

> 用户配置 20 并发，MySQL 100 张分表同步到 ODPS：

```
1. Reader.split() → 100 个 Task
2. adjustChannelNumber() → needChannelNumber = 20
3. JobAssignUtil.assignFairly() → 20/5 = 4 个 TaskGroup
4. 每个 TaskGroup 负责 5 个并发 Channel，共运行 100 个 Task
```

---

## 6. 配置系统

### 6.1 Configuration 类

`common/.../util/Configuration.java` 是贯穿全局的配置对象：

- 基于 JSON 的树形结构
- 支持点路径访问：`job.content[0].reader.name`
- 支持 merge、clone、变量替换（`${var}`）

### 6.2 三层配置合并

```
用户 Job JSON
    ↓ merge
core.json（框架默认配置）
    ↓ merge
plugin.json（Reader/Writer 插件元信息）
    ↓
最终 Configuration
```

`ConfigParser.parse()` 流程：

1. 加载 Job JSON（本地文件或 HTTP URL）
2. `SecretUtil.decryptSecretKey()` 解密敏感字段
3. 合并 `conf/core.json`
4. 扫描 `plugin/reader/` 和 `plugin/writer/`，加载所需插件的 `plugin.json`
5. 以 `plugin.{reader|writer}.{name}` 为 key 注册

### 6.3 Job JSON 结构

```json
{
  "job": {
    "setting": {
      "speed": { "channel": 5, "byte": 1048576, "record": 10000 },
      "errorLimit": { "record": 0, "percentage": 0.02 }
    },
    "content": [{
      "reader": { "name": "mysqlreader", "parameter": { ... } },
      "writer": { "name": "mysqlwriter", "parameter": { ... } },
      "transformer": [{ "name": "dx_substr", "parameter": { ... } }]
    }]
  }
}
```

`split()` 之后，`job.content` 变为 Task 级配置列表：

```json
{
  "taskId": 0,
  "reader": { "name": "mysqlreader", "parameter": { /* 切分后的子配置 */ } },
  "writer": { "name": "mysqlwriter", "parameter": { /* 对应子配置 */ } }
}
```

### 6.4 核心配置项（core.json）

| 配置路径 | 默认值 | 含义 |
|---------|--------|------|
| `core.transport.channel.class` | MemoryChannel | Channel 实现类 |
| `core.transport.channel.capacity` | 512 | Channel 队列容量（条数） |
| `core.transport.channel.byteCapacity` | 64MB | Channel 内存上限 |
| `core.transport.channel.speed.byte` | -1（不限） | 单 Channel 字节限速 |
| `core.transport.channel.speed.record` | -1（不限） | 单 Channel 记录限速 |
| `core.transport.exchanger.bufferSize` | 32 | Exchanger 批量缓冲大小 |
| `core.container.taskGroup.channel` | 5 | 每 TaskGroup 最大并发 |
| `core.container.taskGroup.sleepInterval` | 100ms | 调度轮询间隔 |

---

## 7. 插件体系与加载机制

### 7.1 SPI 接口

每个插件必须继承 `Reader` 或 `Writer`，并实现 **static 内部类** `Job` 和 `Task`：

```java
// Reader 插件
public abstract class Reader {
    public static abstract class Job extends AbstractJobPlugin {
        public abstract List<Configuration> split(int adviceNumber);
    }
    public static abstract class Task extends AbstractTaskPlugin {
        public abstract void startRead(RecordSender recordSender);
    }
}

// Writer 插件
public abstract class Writer {
    public static abstract class Job extends AbstractJobPlugin {
        public abstract List<Configuration> split(int mandatoryNumber);
    }
    public static abstract class Task extends AbstractTaskPlugin {
        public abstract void startWrite(RecordReceiver recordReceiver);
        public boolean supportFailOver() { return false; }
    }
}
```

**split 语义差异**：

- **Reader.Job.split(adviceNumber)**：框架建议的并发数，插件应切分 **≥ adviceNumber** 个 Task
- **Writer.Job.split(mandatoryNumber)**：必须严格按 Reader 切分数切分，保证 1:1 通道模型

### 7.2 插件注册（plugin.json）

```json
{
  "name": "mysqlreader",
  "class": "com.alibaba.datax.plugin.reader.mysqlreader.MysqlReader",
  "description": "MySQL Reader",
  "developer": "alibaba"
}
```

打包后目录：

```
plugin/reader/mysqlreader/
├── plugin.json
├── plugin_job_template.json
├── mysqlreader-0.0.1-SNAPSHOT.jar
└── libs/    # 传递依赖
```

### 7.3 类加载原理（LoadUtil）

`LoadUtil` 是插件加载的核心：

```
1. pluginRegisterCenter 存储所有 plugin.{type}.{name} 配置
2. getJarLoader(type, name) → 为每个插件创建独立 JarLoader（URLClassLoader）
3. loadPluginClass() → 反射加载 {class}$Job 或 {class}$Task 内部类
4. loadPluginRunner() → 包装为 ReaderRunner / WriterRunner
```

**命名约定**：框架通过 `pluginConf.getString("class") + "$" + "Job"/"Task"` 定位内部类，因此 **Job 和 Task 必须是 static 内部类**。

**类加载隔离**：每个插件使用独立 `JarLoader`，通过 `Thread.setContextClassLoader()` 在 Task 线程中切换，避免依赖冲突。

### 7.4 插件复用模式

大量插件通过公共 util 模块复用逻辑：

| 模块 | 适用场景 | 示例 |
|------|---------|------|
| `plugin-rdbms-util` | 关系型数据库 | mysqlreader → `CommonRdbmsReader` |
| `plugin-unstructured-storage-util` | 文件/HDFS/FTP | hdfsreader |

以 `MysqlReader` 为例，Job/Task 仅做薄封装，核心逻辑委托给 `CommonRdbmsReader`：

```java
public class MysqlReader extends Reader {
    public static class Job extends Reader.Job {
        private CommonRdbmsReader.Job commonRdbmsReaderJob;
        public List<Configuration> split(int adviceNumber) {
            return commonRdbmsReaderJob.split(originalConfig, adviceNumber);
        }
    }
    public static class Task extends Reader.Task {
        public void startRead(RecordSender recordSender) {
            commonRdbmsReaderTask.startRead(recordSender);
        }
    }
}
```

RDBMS 切分策略在 `ReaderSplitUtil.doSplit()` 中实现，支持按表、按主键范围等方式切分。

### 7.5 插件与框架通信接口

| 接口 | 方向 | 用途 |
|------|------|------|
| `RecordSender` | Reader → Framework | `createRecord()`, `sendToWriter()`, `terminate()` |
| `RecordReceiver` | Framework → Writer | `getFromReader()` |
| `TaskPluginCollector` | 插件 → Framework | 脏数据收集、统计上报 |
| `JobPluginCollector` | Job 插件 → Framework | Job 级汇报 |

---

## 8. 数据模型与传输层

### 8.1 Record 与 Column

DataX 的统一数据行模型：

```
Record（一行数据）
├── Column[0]: StringColumn / LongColumn / DateColumn / DoubleColumn / BoolColumn / BytesColumn
├── Column[1]: ...
└── meta: Map<String, String>  # 元信息
```

`DefaultRecord` 实现：

- 内部 `List<Column>` 存储列
- 维护 `byteSize`（网络传输大小）和 `memorySize`（内存占用，含对象头）
- 内存大小用于 Channel 流控

### 8.2 数据传输全链路

```
Reader.Task
  │ createRecord() + 填充 Column
  │ sendToWriter(record)
  ▼
BufferedRecordExchanger（RecordSender 实现）
  │ 本地 buffer 攒批（默认 32 条）
  │ flush() → channel.pushAll()
  ▼
MemoryChannel（ArrayBlockingQueue<Record>）
  │ 双维度流控：条数 capacity + 内存 byteCapacity
  │ 字节/记录限速
  ▼
BufferedRecordExchanger（RecordReceiver 实现）
  │ channel.pullAll() → 本地 buffer
  │ getFromReader() → 逐条返回
  ▼
Writer.Task
  │ 写入目的端
```

### 8.3 BufferedRecordExchanger 批量缓冲

Reader 和 Writer 各持有一个 `BufferedRecordExchanger` 实例（分别实现 `RecordSender` 和 `RecordReceiver），通过同一个 `Channel` 对象通信。

**发送端（Reader 侧）**：

```java
public void sendToWriter(Record record) {
    // 单条记录超过 byteCapacity → 收集为脏数据
    boolean isFull = (bufferIndex >= bufferSize || memoryBytes + record.getMemorySize() > byteCapacity);
    if (isFull) flush();
    buffer.add(record);
}

public void flush() {
    channel.pushAll(buffer);  // 批量推入 Channel
    buffer.clear();
}

public void terminate() {
    flush();
    channel.pushTerminate(TerminateRecord.get());  // 发送结束信号
}
```

**接收端（Writer 侧）**：

```java
public Record getFromReader() {
    if (bufferIndex >= buffer.size()) receive();  // 从 Channel 批量拉取
    Record record = buffer.get(bufferIndex++);
    if (record instanceof TerminateRecord) return null;  // 结束信号
    return record;
}
```

### 8.4 MemoryChannel 底层实现

`MemoryChannel` 基于 `ArrayBlockingQueue<Record>` + `ReentrantLock` + `Condition`：

**doPushAll（批量写入）**：

```java
lock.lockInterruptibly();
while (memoryBytes.get() + bytes > byteCapacity || rs.size() > queue.remainingCapacity()) {
    notSufficient.await(200ms);  // 等待 Writer 消费
}
queue.addAll(rs);
memoryBytes.addAndGet(bytes);
notEmpty.signalAll();
```

**doPullAll（批量读取）**：

```java
lock.lockInterruptibly();
while (queue.drainTo(rs, bufferSize) <= 0) {
    notEmpty.await(200ms);  // 等待 Reader 生产
}
memoryBytes.addAndGet(-bytes);
notSufficient.signalAll();
```

双维度流控：

1. **条数限制**：`capacity`（默认 512 条）
2. **内存限制**：`byteCapacity`（默认 64MB），通过 `Record.getMemorySize()` 累加

### 8.5 Channel 限速机制

`Channel.statPush()` 在每次 push 后执行流控：

```java
// 每隔 flowControlInterval（默认 20ms）检查一次
long currentByteSpeed = (totalBytes_now - totalBytes_last) * 1000 / interval;
if (currentByteSpeed > byteSpeed) {
    sleepTime = currentByteSpeed * interval / byteSpeed - interval;
    Thread.sleep(sleepTime);
}
```

支持字节限速（bps）和记录限速（tps），取较大休眠时间。限速在 **Channel 层** 实现，对插件透明。

### 8.6 结束信号 TerminateRecord

Reader 完成所有数据读取后，调用 `recordSender.terminate()`：

1. flush 剩余 buffer
2. 向 Channel 推送 `TerminateRecord`（单例标记对象）
3. Writer 侧 `getFromReader()` 收到 `TerminateRecord` 返回 `null`，退出读取循环

---

## 9. 调度与并发控制

### 9.1 并发度计算（adjustChannelNumber）

`JobContainer.adjustChannelNumber()` 按优先级确定并发 Channel 数：

```
1. 若配置了 speed.byte → channel数 = 全局byte限速 / 单channel byte限速
2. 若配置了 speed.record → channel数 = 全局record限速 / 单channel record限速
3. 取 1 和 2 的较小值
4. 若以上均未配置 → 使用 speed.channel 直接指定
5. 若均未配置 → 抛异常（必须设置速度）
```

最终 `needChannelNumber = min(needChannelNumber, taskNumber)`，不会超过实际 Task 数。

### 9.2 Task 分配（JobAssignUtil）

`JobAssignUtil.assignFairly()` 公平分配 Task 到 TaskGroup：

1. 读取每个 Task 的 `loadBalanceResourceMark`（资源标识，如库名）
2. 若无资源标识，fake 一个并 **shuffle** 打乱（避免长尾）
3. 按资源标识轮询分配到各 TaskGroup（同库表分散到不同组）
4. 调整每组 Channel 数，余数优先分配给前面的组

### 9.3 TaskGroup 调度循环

`TaskGroupContainer.start()` 主循环（每 100ms 一轮）：

```
while (true) {
    1. 检查已完成 Task 的状态（SUCCESS/FAILED/KILLED）
    2. 失败 Task 支持 Failover 则重新入队
    3. 从待执行队列取 Task，受 channelNumber 限制并发启动
    4. 所有 Task 完成 → 退出
    5. 定期汇报统计信息（默认 10s）
}
```

### 9.4 线程模型

```
JobContainer 主线程
  └── FixedThreadPool(M)          # M = TaskGroup 数量
       └── TaskGroupContainer     # 每组一个线程
            └── TaskExecutor × N  # 每组最多 N 个并发
                 ├── writerThread  # WriterRunner
                 └── readerThread  # ReaderRunner
```

每个 Task 固定 **2 个线程**（1 Reader + 1 Writer），通过 Channel 队列解耦。

---

## 10. 统计监控与脏数据处理

### 10.1 Communication 状态机

每个 Task 维护一个 `Communication` 对象，记录：

| 计数器 | 含义 |
|--------|------|
| `readSucceedRecords/Bytes` | 成功读取 |
| `writeReceivedRecords/Bytes` | 成功写入 |
| `readFailedRecords` | 读取失败（脏数据） |
| `waitReaderTime/WriterTime` | 等待时间 |
| `transformerSucceed/Failed/Filter` | 转换统计 |

状态流转：`RUNNING → SUCCEEDED / FAILED / KILLED`

### 10.2 多级汇报

```
Task Communication
  → TaskGroupContainer Communicator（汇总）
    → JobContainer Communicator（汇总）
      → 日志输出 / 远程上报
```

### 10.3 脏数据处理

插件通过 `TaskPluginCollector.collectDirtyRecord(record, exception)` 上报脏数据。

默认收集器 `StdoutPluginCollector` 将脏数据打印到日志（最多 `maxDirtyNumber` 条，默认 10）。

### 10.4 错误限制

```json
"errorLimit": {
  "record": 0,        // 绝对条数阈值
  "percentage": 0.02  // 百分比阈值
}
```

`ErrorRecordChecker` 在 Job 结束后检查脏数据是否超过阈值。

### 10.5 性能追踪（PerfTrace）

`PerfTrace` 记录各阶段耗时：

- `READ_TASK_INIT/PREPARE/DATA/POST/DESTROY`
- `WRITE_TASK_INIT/PREPARE/DATA/POST/DESTROY`
- `WAIT_READ_TIME / WAIT_WRITE_TIME`
- `TRANSFORMER_TIME`

---

## 11. 容错与 Failover

### 11.1 线程级 Failover

`TaskGroupContainer` 支持 Task 级别重试：

```java
if (taskCommunication.getState() == State.FAILED) {
    if (taskExecutor.supportFailOver() && attemptCount < taskMaxRetryTimes) {
        // 关闭旧 Executor，重置状态，重新入队
        taskQueue.add(taskConfig);
    }
}
```

- 默认最大重试 1 次
- 重试间隔 10s，最大等待 60s
- 仅当 `Writer.Task.supportFailOver()` 返回 `true` 时生效

### 11.2 成功标记策略

```java
// ReaderRunner - 不标记成功
// super.markSuccess(); 这里不能标记为成功

// WriterRunner - 由 Writer 标记成功
super.markSuccess();
```

避免 Reader 先结束而 Writer 仍在写入的竞态。

### 11.3 异常处理

- `JobContainer`：catch 所有 Throwable，设置 Communication 状态为 FAILED，汇报后抛出 `DataXException`
- `Engine.main()`：根据 `FrameworkErrorCode` 映射退出码
- OOM 时主动 `destroy()` + `System.gc()`

---

## 12. Transformer 数据转换

### 12.1 配置方式

```json
"transformer": [{
  "name": "dx_substr",
  "parameter": {
    "columnIndex": 0,
    "paras": ["1", "3"]
  }
}]
```

### 12.2 执行位置

当 Task 配置了 transformer 时，`TaskExecutor` 使用 `BufferedRecordTransformerExchanger` 替代 `BufferedRecordExchanger`：

```
Reader → BufferedRecordTransformerExchanger.sendToWriter()
           ├── 执行 Transformer 链
           └── flush → Channel
Writer ← BufferedRecordTransformerExchanger.getFromReader()
```

转换在 Reader 侧、数据进入 Channel 之前执行，对 Writer 透明。

---

## 13. 完整执行示例

以 `mysqlreader → mysqlwriter` 同步为例，配置 5 并发：

### 时序图

```
用户                Engine           JobContainer        Scheduler         TaskGroupContainer       Reader/Writer
 │                    │                   │                  │                      │                    │
 │─ datax.py job.json─▶│                   │                  │                      │                    │
 │                    │─ ConfigParser ─────▶│                  │                      │                    │
 │                    │─ LoadUtil.bind ────▶│                  │                      │                    │
 │                    │                   │─ init() ─────────│                      │                    │
 │                    │                   │  加载 Reader.Job │                      │                    │
 │                    │                   │  加载 Writer.Job │                      │                    │
 │                    │                   │─ prepare() ──────│                      │                    │
 │                    │                   │─ split(5) ───────│                      │                    │
 │                    │                   │  Reader→10 Task   │                      │                    │
 │                    │                   │  Writer→10 Task   │                      │                    │
 │                    │                   │  merge→10 content│                      │                    │
 │                    │                   │─ schedule() ─────▶│                      │                    │
 │                    │                   │                  │─ assignFairly() ─────▶│                    │
 │                    │                   │                  │  2个TaskGroup         │                    │
 │                    │                   │                  │─ start ThreadPool ───▶│                    │
 │                    │                   │                  │                      │─ 启动5个Task ──────▶│
 │                    │                   │                  │                      │  writerThread.start│
 │                    │                   │                  │                      │  readerThread.start│
 │                    │                   │                  │                      │                    │─ 读写数据
 │                    │                   │                  │                      │◀── 完成 ───────────│
 │                    │                   │                  │◀── 全部完成 ──────────│                    │
 │                    │                   │◀── 完成 ──────────│                      │                    │
 │                    │                   │─ post/destroy ───│                      │                    │
 │◀── 打印统计信息 ─────│                   │                  │                      │                    │
```

### 数据流

```
MySQL ResultSet
  → Reader.Task 读取一行
  → RecordSender.createRecord() + addColumn()
  → BufferedRecordExchanger.sendToWriter()
  → [buffer 满 32 条] flush()
  → MemoryChannel.pushAll() [ArrayBlockingQueue]
  → BufferedRecordExchanger.getFromReader()
  → Writer.Task 写入
  → MySQL PreparedStatement.execute()
```

---

## 14. 关键源码索引

### 框架核心

| 类 | 路径 | 职责 |
|----|------|------|
| `Engine` | `core/.../Engine.java` | 程序入口，容器选择与启动 |
| `JobContainer` | `core/.../job/JobContainer.java` | Job 生命周期管理 |
| `TaskGroupContainer` | `core/.../taskgroup/TaskGroupContainer.java` | TaskGroup 调度与 Task 执行 |
| `StandAloneScheduler` | `core/.../job/scheduler/processinner/StandAloneScheduler.java` | 独立模式调度器 |
| `ProcessInnerScheduler` | `core/.../job/scheduler/processinner/ProcessInnerScheduler.java` | 进程内线程池调度 |
| `JobAssignUtil` | `core/.../container/util/JobAssignUtil.java` | Task 公平分配 |
| `ConfigParser` | `core/.../util/ConfigParser.java` | 配置解析与合并 |
| `LoadUtil` | `core/.../util/container/LoadUtil.java` | 插件加载 |

### 传输层

| 类 | 路径 | 职责 |
|----|------|------|
| `Channel` | `core/.../transport/channel/Channel.java` | Channel 抽象，统计与限速 |
| `MemoryChannel` | `core/.../transport/channel/memory/MemoryChannel.java` | 内存队列实现 |
| `BufferedRecordExchanger` | `core/.../transport/exchanger/BufferedRecordExchanger.java` | 批量缓冲交换器 |
| `DefaultRecord` | `core/.../transport/record/DefaultRecord.java` | 默认 Record 实现 |
| `TerminateRecord` | `core/.../transport/record/TerminateRecord.java` | 结束信号 |

### 执行器

| 类 | 路径 | 职责 |
|----|------|------|
| `ReaderRunner` | `core/.../taskgroup/runner/ReaderRunner.java` | Reader Task 线程执行器 |
| `WriterRunner` | `core/.../taskgroup/runner/WriterRunner.java` | Writer Task 线程执行器 |

### 公共模块

| 类 | 路径 | 职责 |
|----|------|------|
| `Reader` / `Writer` | `common/.../spi/` | 插件 SPI 接口 |
| `Configuration` | `common/.../util/Configuration.java` | 统一配置对象 |
| `RecordSender` / `RecordReceiver` | `common/.../plugin/` | 插件数据交互接口 |
| `CommonRdbmsReader` | `plugin-rdbms-util/.../CommonRdbmsReader.java` | RDBMS 公共读写逻辑 |

### 启动脚本

| 文件 | 路径 | 职责 |
|------|------|------|
| `datax.py` | `core/src/main/bin/datax.py` | Python 启动器 |
| `core.json` | `core/src/main/conf/core.json` | 框架默认配置 |

---

## 附录：架构设计要点总结

1. **Framework + Plugin 解耦**：框架处理同步共性（缓冲、流控、并发、统计），插件只关注数据源读写
2. **1:1 Channel 模型**：强制 Reader/Writer Task 对等，简化数据管道设计
3. **两级容器**：JobContainer（全局调度）+ TaskGroupContainer（局部并发），支持水平扩展
4. **插件类加载隔离**：每插件独立 ClassLoader，通过 Thread ContextClassLoader 切换
5. **双维度流控**：条数 + 内存字节双重限制，防止 OOM
6. **批量缓冲**：Exchanger 层攒批（32 条）+ Channel 层队列，减少锁竞争
7. **Writer 主导成功**：避免 Reader/Writer 竞态，保证数据完整性
8. **资源感知调度**：按 loadBalanceResourceMark 分散同库表 Task，均衡负载
