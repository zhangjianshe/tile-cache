# tile-cache

集中式瓦片缓存服务。`cis-map` 等生产者只负责查询和生成瓦片，所有持久化 SQLite 读写由本服务统一管理，从而避免多实例直接写同一 SQLite 文件。

## 特性

- Axum HTTP 二进制瓦片接口
- 管理根目录下多个瓦片数据库
- 完全兼容 `cn.mapway.common.geo.tools.TileTools` 的目录、分片和表结构
- 每个 `.s` SQLite 分片拥有独立 WAL 和单写队列，不同分片可并行写入
- 有界写队列和过载保护
- 多连接并发读取
- 幂等瓦片覆盖写入
- 标准 XYZ 风格的瓦片 URL
- 数据库和影像瓦片目录的查询、统计与删除
- 自动清理长期未访问瓦片
- 内置数据库数量、磁盘占用和最近 24 小时访问统计 Dashboard
- 可选 Bearer Token 鉴权
- amd64/arm64 Docker 发布

## 运行

```bash
cargo run

# 生产环境应为写入和删除操作启用鉴权
TILE_CACHE_AUTH_TOKEN=change-me cargo run --release
```

默认监听 `0.0.0.0:7601`，Tokio 异步运行时默认使用 4 个工作线程，避免在多核服务器上空载时按全部逻辑 CPU 创建线程。完整参数可通过 `tile-cache --help` 查看。

Dashboard 地址为 `http://127.0.0.1:7601/`，GET 请求无需 Token。瓦片写入和删除管理 API 使用 Bearer Token 鉴权。

Dashboard 每 30 秒刷新，展示数据库数量、磁盘占用、当前进程内存占用、线程数，以及最近 24 小时逐小时 GET/PUT、命中/未命中和读写流量。进程资源指标从 Linux `/proc/self/status` 实时读取，不写入数据库。请求先在内存中计数，每 60 秒批量写入实例私有的 `./config/tile-cache-meta.db`，不会给瓦片 SQLite 增加统计写入。服务启动时会恢复最近 90 天的小时数据。

## API

XYZ URL 为 `/tiles/{databaseSha256}/{itemSha256}/{z}/{x}/{y}.{format}`。

瓦片内容不能为空：PUT 0 字节数据返回 `400 Bad Request`；GET 遇到历史空记录按缓存未命中返回 `404 Not Found`。客户端可据此决定是否生成并回写瓦片。

数据库使用 `TileTools` 的磁盘格式：

```text
<root>/<db前4位>/<db剩余部分>/
└── <item前4位>/<item剩余部分>/
    └── <max(z,9)对应字母>/
        └── <字母>_<x/256>_<y/256>.s
            └── <z对应字母>_<x/64>_<y/64>(ID, Data, X, Y, F)
```

`databaseSha256` 和 `itemSha256` 本身已经是摘要，不再重复计算 MD5。两者都采用 `<前4位>/<剩余部分>` 两级散列目录，在控制单目录条目数量的同时保留数据库与影像之间的归属关系。

```bash
# 健康检查（无需鉴权）
curl http://127.0.0.1:7601/health

# 插入或覆盖瓦片
curl -X PUT --data-binary @tile.png \
  -H 'authorization: Bearer change-me' \
  http://127.0.0.1:7601/tiles/DB_SHA256/ITEM_SHA256/10/812/420.png

# 查询瓦片（无需鉴权）
curl -o tile.png \
  http://127.0.0.1:7601/tiles/DB_SHA256/ITEM_SHA256/10/812/420.png

# 列出全部瓦片数据库
curl \
  http://127.0.0.1:7601/api/v1/databases

# 查询数据库中的影像瓦片目录及容量
curl \
  http://127.0.0.1:7601/api/v1/databases/DB_SHA256

# 删除一个影像瓦片目录及其全部 .s 分片
curl -X DELETE -H 'authorization: Bearer change-me' \
  http://127.0.0.1:7601/api/v1/databases/DB_SHA256/tilesets/ITEM_SHA256

# 删除整个瓦片数据库目录
curl -X DELETE -H 'authorization: Bearer change-me' \
  http://127.0.0.1:7601/api/v1/databases/DB_SHA256

# 立即清理 7 天未访问的数据
curl -X DELETE -H 'authorization: Bearer change-me' \
  'http://127.0.0.1:7601/api/v1/admin/cleanup?days=7'
```

## Docker

```bash
docker run --rm -p 7601:7601 \
  -e TILE_CACHE_AUTH_TOKEN=change-me \
  -v tile-cache-data:/app/tiledata \
  -v tile-cache-config:/app/config \
  harbor.cangling.cn:22002/cangling/tile-cache:latest
```

重要环境变量：

| 变量 | 缺省值 | 说明 |
| --- | --- | --- |
| `TILE_CACHE_ADDR` | `0.0.0.0:7601` | 监听地址 |
| `TILE_CACHE_WORKER_THREADS` | `4` | Tokio 异步运行时工作线程数；高并发实例可按压测结果增大 |
| `TILE_CACHE_ROOT` | `./tiledata` | 当前工作目录下的瓦片数据库根目录；Docker Volume 为 `/app/tiledata` |
| `TILE_CACHE_CONFIG_DIR` | `./config` | 当前实例的管理数据库目录；Docker Volume 为 `/app/config`，多实例不得共享 |
| `TILE_CACHE_AUTH_TOKEN` | 空 | PUT/DELETE API 的 Bearer Token，空表示关闭写操作鉴权；GET 始终公开 |
| `TILE_CACHE_MAX_TILE_BYTES` | `33554432` | 单瓦片最大字节数 |
| `TILE_CACHE_WRITE_QUEUE` | `4096` | 有界写队列容量 |
| `TILE_CACHE_READ_CONNECTIONS` | `16` | SQLite 读连接数 |
| `TILE_CACHE_RETENTION_DAYS` | `7` | 自动清理天数，`0` 禁用 |

## 发布

发布流程参考 `cangling-broker`：只在推送 `v*` 标签时运行 GitHub Actions，分别在原生 amd64 和 arm64 runner 上测试、编译，然后构建多架构镜像并推送到 Docker Hub 和 Harbor，同时上传两个架构的镜像压缩包。

```bash
./release.sh
```

仓库需要配置：`DOCKERHUB_USERNAME`、`DOCKERHUB_TOKEN`、`HARBOR_USERNAME`、`HARBOR_PASSWORD`、`CANGLING_TOKEN`、`SOFTWARE_TOKEN`。
