# dfly-rdr

> **专为 Dragonfly 与现代 Redis (7.0+) 打造的高性能 RDB / Snapshot 内存分析与大 Key 诊断利器**  
> 基于经典的 `xueqiu/rdr` 深度重构与升级，全面兼容 Dragonfly Snapshot 及 Redis 7.0+ 全新 Listpack 紧凑编码。

---

## 📖 项目背景 (Background)

原版 [xueqiu/rdr](https://github.com/xueqiu/rdr) 是一款出色的 Go 语言 RDB 文件解析与可视化工具，但因年代久远已停更多年。当用于分析现代 **Dragonfly** 或 **Redis 7.0+** 生成的 RDB / DFS 快照时，会遇到严重的兼容性与稳定性缺陷：
1. **未知类型崩溃**：Redis 7.0 及 Dragonfly 广泛使用了 **Listpack** 替代旧的 ziplist 编码，原版在遇到新类型时会直接抛错中断，例如：
   ```text
   decode rdbfile err: rdb: unknown object type 17 for key ...
   ```
2. **大文件“秒完成”与 Web 白屏 Bug**：原版在 `show` 命令中存在异步竞态设计缺陷，几十 GB 的大文件尚未解析完成就抢先输出 `parsing finished`，导致运维人员访问 Web 页面时呈现空内容的纯白屏；
3. **空指针 Crash**：原版在极端场景（如部分空流或缺失结构上下文）下缺少防御性校验，极易发生 nil pointer panic。

**dfly-rdr** 彻底解决了上述问题，不仅将 RDB 解析器内核升级至 **RDB Version 11**，完整实现了 Listpack 系列全量解码器，还对 Web 展示服务、解析同步机制进行了工程级加固，成为运维人员排查 Dragonfly / Redis 大 Key 和内存分布的可靠工具。

---

## ✨ 核心升级与对比 (Features & Differences)

| 功能特性 / 特征 | 原版 `rdr` (xueqiu) | **dfly-rdr** (本项目) | 说明 |
| :--- | :---: | :---: | :--- |
| **Dragonfly Snapshot 兼容** | ❌ 报错崩溃 | ✅ **完美兼容** | 全面支持 Dragonfly 生成的 RDB/快照文件 |
| **最高支持 RDB 版本** | RDB 9 (Redis 5.0) | ✅ **RDB 11 (Redis 7.0+)** | 向上兼容高版本 RDB 规范 |
| **Hash Listpack (Type 16)** | ❌ 不支持 | ✅ **完整解码** | 支持紧凑 Listpack 哈希解析与内存估算 |
| **ZSet Listpack (Type 17)** | ❌ 报错 `unknown type 17` | ✅ **完整解码** | 彻底解决 Dragonfly 最常见的 Type 17 崩溃报错 |
| **Quicklist v2 (Type 18)** | ❌ 不支持 | ✅ **完整解码** | 支持包含 Listpack 容器节点的新版双端链表 |
| **Set Listpack (Type 20)** | ❌ 不支持 | ✅ **完整解码** | 支持紧凑 Listpack 集合解析 |
| **Stream Listpack (19 & 21)**| ❌ 不支持 | ✅ **完整解码** | 支持高版本 Stream 流数据结构解析 |
| **解析状态与提示准确性** | ❌ 虚假 finished，过早提示 | ✅ **完全同步阻塞** | 初始文件全部分析完毕后才提示完成，避免误导 |
| **Web 访问防白屏机制** | ❌ 0 字节纯白屏 | ✅ **优雅加载过渡** | 杜绝空白屏困惑，展示状态友好 |
| **跨平台静态二进制** | 仅老旧版本 | ✅ **单文件无依赖** | Linux x86_64 静态编译，开箱即用 |

---

## 🚀 快速上手 (Quick Start)

### 1. 下载二进制

你可以直接前往 [Releases 页面](https://github.com/boomballa/dfly-rdr/releases) 或直接使用仓库根目录下 `cmd/` 预编译的可执行文件：

- **Linux (x86_64)**: `cmd/dfly-rdr-linux` (静态链接，无需任何动态库，适配主流 CentOS / Ubuntu / Debian 等)
- **macOS**: `cmd/dfly-rdr`

```bash
# 赋予执行权限
chmod +x cmd/dfly-rdr-linux
```

---

### 2. 核心命令使用

#### ① `show` 命令：启动 Web 仪表盘可视化分析（推荐）

分析单个或多个 RDB 文件，并在指定端口启动可视化 Web 报表服务：

```bash
# 本地测试 (macOS)
./cmd/dfly-rdr show -p 8080 ./dump.rdb

# 服务器运行 (Linux)
./cmd/dfly-rdr-linux show -p 39373 ./bjrec-user-recommend-cmpri.rdb
```

**运行过程**：
1. 控制台首先打印 `start parsing...` 与 `start to parse <filename>`；
2. 程序对文件进行深度扫描与内存指标聚合计算；
3. 解析完毕后输出 `parse <filename> done` 与 `parsing finished, please access http://{$IP}:<port>`；
4. 浏览器访问 `http://<服务器IP>:<port>`，即可直观查阅完整的图表与排行大屏。

> **Web 看板包含核心指标**：
> - **Key 数量分布 (按类型饼图)**
> - **内存占用分布 (按类型饼图，精确到 MB/GB)**
> - **大 Key Top 100 排行榜 (Key 名称、数据类型、占用字节大小、子元素数量)**
> - **Key 前缀智能聚合 (按业务前缀归纳 Key 数量与总内存消耗)**
> - **集群 Slot 槽位分布统计**

---

#### ② `dump` 命令：输出全量统计 JSON

如果需要将分析结果对接至监控系统、日志审计或自动化告警，可使用 `dump` 输出结构化 JSON：

```bash
./cmd/dfly-rdr-linux dump ./dump.rdb > rdb_report.json
```

---

#### ③ `keys` 命令：提取并导出全量 Key 列表

```bash
./cmd/dfly-rdr-linux keys ./dump.rdb > all_keys.txt
```

---

## 🛠 源码构建与开发 (Build from Source)

本项目采用 Go Modules 管理依赖，并内置了 Git Commit 自动化构建钩子。

### 环境要求
- Go 1.18+

### 本地编译

```bash
# 编译本地版本 (macOS / Linux)
go build -o cmd/dfly-rdr main.go

# 交叉编译 Linux 64位无依赖静态二进制
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cmd/dfly-rdr-linux main.go
```

### 自动化构建钩子 (Git Hook)
仓库包含 `.git/hooks/post-commit`。配置后，每次执行 `git commit` 时，系统将自动触发重新编译并更新 `cmd/dfly-rdr` 和 `cmd/dfly-rdr-linux`，确保发布的二进制始终与代码保持同步。

---

## 📄 开源许可证 (License)

本项目遵循 [Apache License 2.0](LICENSE) 协议。欢迎提交 Issue 与 Pull Request！
