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
- SQLite 管理目录，数据库和图层列表无需扫描瓦片磁盘
- 数据库与图层的分页、名称/ID 查询、显示名称修改和物理删除
- 数据库与图层独立的自动回收保护，默认允许回收
- 自动清理长期未访问缓存，并保留最近 90 天的逐项清理历史
- 进程内按字节计量的瓦片 LRU，减少热点瓦片反复读取 SQLite 和磁盘
- 内置运行概览、缓存数据预览和清理策略 Dashboard
- OpenLayers 资源内置，支持完全离线的数据预览
- GET 公开，写入与管理 API 支持 Bearer Token；Dashboard 支持管理员会话
- amd64/arm64 Docker 发布

## 运行

```bash
cargo run

# 生产环境应为写入和删除操作启用鉴权
TILE_CACHE_AUTH_TOKEN=change-me cargo run --release
```

默认监听 `0.0.0.0:7601`，Tokio 异步运行时默认使用 4 个工作线程，避免在多核服务器上空载时按全部逻辑 CPU 创建线程。完整参数可通过 `tile-cache --help` 查看。

服务启动时会以 INFO 日志打印全部生效配置；鉴权只打印是否启用，不会输出 Token 明文。

Dashboard 地址为 `http://127.0.0.1:7601/`，GET 请求无需 Token。瓦片写入和删除管理 API 使用 Bearer Token 鉴权。

Dashboard 每 30 秒刷新，展示数据库数量、磁盘占用、当前进程内存占用、线程数，以及最近 24 小时逐小时 GET/PUT、命中/未命中和读写流量。进程资源指标从 Linux `/proc/self/status` 实时读取，不写入数据库。请求先在内存中计数，每 60 秒批量写入实例私有的 `./config/tile-cache-meta.db`，不会给瓦片 SQLite 增加统计写入。服务启动时会恢复最近 90 天的小时数据。

Dashboard 包含三个功能区：

- **运行概览**：数据库数量、磁盘与内存占用、线程数、最近 24 小时 GET/PUT 折线趋势、命中率及读写流量。
- **数据预览**：分页查询数据库和图层，使用本地 OpenLayers 浏览缓存瓦片，并执行重命名、回收策略设置和删除操作。
- **清理策略**：动态修改缓存保留天数、每日清理小时和空闲分片关闭时间，立即执行清理并分页查看清理历史。

清理设置与执行记录保存在实例私有的 `./config/tile-cache-meta.db`；读取设置和历史无需 Token，保存、立即清理和数据管理操作需要管理员会话或 Bearer Token。

“数据预览”页按数据库和数据图层浏览现有缓存。数据库和图层列表支持名称或 ID 查询及服务端分页，图层还可筛选“仅显示不可回收图层”；受保护图层名称前显示锁图标。选择图层后使用 OpenLayers 自动定位并预览 PNG、JPEG、WebP 或 MVT/PBF 瓦片。OpenLayers 10.2.0 的 JS/CSS 已嵌入可执行程序，由 `/assets/openlayers/` 提供，不依赖 CDN 或互联网。预览只调用公开的 GET API，不会生成或写入瓦片。

预览页顶部把管理操作分为“数据库”和“图层”两组，每组提供重命名、允许自动回收复选框和删除按钮。重命名只改变管理目录中的显示名称，原始 SHA256、存储目录和 XYZ URL 保持不变；列表摘要仍显示原有格式、瓦片数量和容量，完整原始 ID 可通过提示查看。删除操作必须二次确认，服务会先关闭对应 SQLite 分片连接、删除物理目录，再同步管理目录并刷新页面统计。

数据库与图层目录保存在实例管理库 `config/tile-cache-meta.db`。列表、Dashboard 和数据预览直接查询管理表，不会在每次访问时扫描 `.s` 分片；瓦片 PUT、图层删除和数据库删除会增量维护数量、容量、层级、范围、回收策略及更新时间。升级已有数据时首次启动会自动建立目录，管理员也可通过重建接口手动校准；重建会保留显示名称和回收保护状态。

PUT 在瓦片分片提交成功后即返回，目录增量通过容量为 65,536 的有界队列在内存中按数据库和图层合并，每 60 秒批量写入；队列满时生产者会等待而不会丢失统计。Dashboard 的数量、容量和范围因此最多延迟约一分钟，但管理 SQLite 的抖动不会把已经成功写入的瓦片误报为失败。重命名、回收策略和删除等管理操作会先强制刷新待处理目录增量，避免操作到过期记录。

GET 在访问 SQLite 前会查询进程内瓦片 LRU。LRU 按瓦片实际字节数限制容量，缺省为 512 MiB；PUT 成功后直接写入缓存，覆盖、删除图层或删除数据库时同步失效相关条目。404/空瓦片不会进入缓存，单块超过 2 MiB 的瓦片缺省不进入 LRU，避免少数异常大对象挤出热点数据。该缓存是每个实例独立的性能缓存，不影响共享瓦片数据库的一致性；设置 `TILE_CACHE_MEMORY_CACHE_BYTES=0` 可关闭。

瓦片写入不预查询旧记录：先执行 `INSERT OR IGNORE`，键冲突时直接 `UPDATE` 覆盖。新增瓦片可以直接增量统计；发生覆盖的图层会在下一次分钟级目录刷新时从分片重新汇总数量和容量，保证 Dashboard 最终准确而不阻塞 PUT 主路径。

管理数据库位于实例本地的 `config` 目录，不放在共享的 `tiledata` 中。多实例可以共享或访问同一瓦片数据服务，但每个 `tile-cache` 实例的管理数据库和运行统计应独立保存，不能让多个实例直接并发写同一个管理 SQLite。

Dashboard 未登录时全部管理设置只读。首次启动且管理数据库中没有管理员时，服务会自动生成随机初始密码并在启动日志中只显示一次；请用它从页面左下角“管理员登录”。登录会话两小时无操作后失效，重置密码会注销全部旧会话：

Docker 部署可以设置 `TILE_CACHE_ADMIN_PASSWORD` 初始化管理员密码。该变量只在管理员不存在时使用，容器重启不会覆盖已经保存的密码。生产环境优先通过 Compose secret、Kubernetes Secret 或受保护的环境文件注入，不要把明文密码提交到仓库。

```bash
# 指定密码
tile-cache --config-dir ./config reset-password -p 'new-password'

# 省略密码时自动生成并只显示一次
tile-cache --config-dir ./config reset-password
```

网页登录不会替代服务间 Bearer Token；现有 PUT/DELETE 客户端仍可继续使用 `TILE_CACHE_AUTH_TOKEN`。

## API

XYZ URL 为 `/tiles/{databaseSha256}/{itemSha256}/{z}/{x}/{y}.{format}`。

瓦片内容不能为空：PUT 0 字节数据返回 `400 Bad Request`；GET 遇到历史空记录按缓存未命中返回 `404 Not Found`。客户端可据此决定是否生成并回写瓦片。

Dashboard 地图预览会在瓦片 URL 上附加 `preview=1`。仅在该模式下，缺失的栅格瓦片返回内置的浅灰色 `NOT CACHED` 水印占位图、HTTP 200 和 `X-Tile-Cache-Miss: true`，避免浏览器控制台产生大量图片 404，并明确标识未缓存区域；普通客户端请求仍返回 404，缓存生成判断不受影响。

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
  -H 'x-tile-database-name: %E6%88%90%E9%83%BD%E5%BD%B1%E5%83%8F%E5%BA%93' \
  -H 'x-tile-layer-name: ZY302_PMS_20260525.tif' \
  http://127.0.0.1:7601/tiles/DB_SHA256/ITEM_SHA256/10/812/420.png

# Seed 批量插入或覆盖瓦片（单次最多 256 块，data 为 Base64）
curl -X POST -H 'authorization: Bearer change-me' \
  -H 'content-type: application/json' \
  -d '{"database":"DB_SHA256","item":"ITEM_SHA256","format":"png","database_name":"成都影像库","layer_name":"ZY302_PMS_20260525.tif","tiles":[{"z":10,"x":812,"y":420,"data":"iVBORw0KGgo="}]}' \
  http://127.0.0.1:7601/api/v1/tiles/batch

# 查询瓦片（无需鉴权）
curl -o tile.png \
  http://127.0.0.1:7601/tiles/DB_SHA256/ITEM_SHA256/10/812/420.png

# 分页查询瓦片数据库（名称或 ID）
curl \
  'http://127.0.0.1:7601/api/v1/databases?page=1&page_size=50&q=成都'

# 分页查询数据库中的图层（名称或 ID）
curl \
  'http://127.0.0.1:7601/api/v1/databases/DB_SHA256?page=1&page_size=100&q=2026'

# 仅查询不可自动回收的图层，可与 q 和分页参数组合
curl \
  'http://127.0.0.1:7601/api/v1/databases/DB_SHA256?page=1&page_size=100&revocable=false'

# 修改数据库显示名称（不改变 SHA256 和目录）
curl -X PATCH -H 'authorization: Bearer change-me' \
  -H 'content-type: application/json' \
  -d '{"name":"成都遥感影像库"}' \
  http://127.0.0.1:7601/api/v1/databases/DB_SHA256

# 修改图层显示名称
curl -X PATCH -H 'authorization: Bearer change-me' \
  -H 'content-type: application/json' \
  -d '{"name":"2026年5月卫星影像"}' \
  http://127.0.0.1:7601/api/v1/databases/DB_SHA256/tilesets/ITEM_SHA256

# 从磁盘重新建立管理目录（保留显示名称）
curl -X POST -H 'authorization: Bearer change-me' \
  http://127.0.0.1:7601/api/v1/admin/catalog/rebuild

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

PUT 可以携带经过 UTF-8 百分号编码的 `x-tile-database-name` 和
`x-tile-layer-name`。服务在写入首块瓦片的同一事务中建立管理目录并设置显示名；
以后仅在显示名仍为空时补充名称，不会覆盖 Dashboard 中的人工改名。这样 Seed
无需先调用单独的重命名接口，也不会出现瓦片已经创建但名称尚未登记的中间状态。

批量接口专供离线 Seed 使用，将同一数据库、同一图层的多块瓦片合并提交；服务端
仍按瓦片坐标插入或覆盖，并复用分片写入队列和事务合并。接口单次最多接收 256 块，
CIS 当前按最多 128 块且原始瓦片数据最多 16 MiB 自适应切批。地图浏览和按需生成
继续使用 XYZ URL 的单瓦片 PUT，避免为单次交互引入等待；CIS 遇到批量请求失败时
会自动降级为逐瓦片 PUT。

## Release 写入压测（2026-09-24）

本次测试使用 `cargo build --release` 生成的二进制和全新的临时数据目录，通过本机
回环地址访问。服务配置为 4 个 Tokio worker、每分片 4 个读连接，测试机为
Intel Core i9-14900HX（32 个逻辑 CPU）、31 GiB 内存、NVMe ext4。每块瓦片为
16 KiB；测试前预热 64 次单 PUT 和 1 个 128 块批次，正式结果如下：

| 场景 | 请求方式 | 数据量 | 并发 | 耗时 | 吞吐量 | 平均延迟 | P95 | HTTP 错误 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 普通按需写入 | 单瓦片 PUT | 20,000 块 | 32 | 15.85 s | 1,262 块/s（19.72 MiB/s） | 25.28 ms | 54.83 ms | 0 |
| CIS Seed | 每批 128 块 POST | 25,600 块 | 1 个批量请求流 | 4.38 s | 5,839 块/s（91.23 MiB/s） | 21.89 ms/批 | 55.04 ms/批 | 0 |

Seed 批量写入在当前配置下是单瓦片 PUT 吞吐量的约 **4.63 倍**，同时把 25,600 次
HTTP 请求减少为 200 次。这里的并发 `1` 与当前 CIS Seed 客户端的实际行为一致：
客户端顺序提交批次，每个批次内部由 tile-cache 并行分发到分片写队列。以上是单机、
本地回环和当前硬件上的一次工程基准，不作为跨机器的固定容量承诺；磁盘、瓦片空间
分布、多个 cis-map 实例以及后台负载都会影响结果。

清理历史保存在实例本地的 `config/tile-cache-meta.db` 中，记录定时清理、手动删除数据库和手动删除图层的操作时间、类型、数据库/图层名称与原始 ID、图层数、瓦片数、释放空间、操作者及执行结果，默认保留 90 天。历史查询接口为：

```text
GET /api/v1/admin/cleanup/history?page=1&page_size=20
```

数据库和图层都支持独立设置是否允许定时自动回收，默认值为 `true`。设置为 `false` 后定时任务不会删除该对象，管理员仍可手动删除。旧管理数据库启动时会自动增加该字段，已有对象按默认值 `true` 处理。

定时清理遵循以下规则：

- 数据库及其全部图层都允许回收时，删除整个过期数据库。
- 数据库不允许整体回收，或者其中包含受保护图层时，只删除该数据库中允许回收的过期图层。
- 不可回收的数据库或图层不会被定时任务删除，但不限制管理员手动删除。
- 自动删除数据库或图层都会写入清理历史。

写接口需要管理员会话或 Bearer Token：

```text
PUT /api/v1/databases/{database}/revocable
PUT /api/v1/databases/{database}/tilesets/{item}/revocable

{"revocable":false}
```

## Docker

```bash
docker run --rm -p 7601:7601 \
  -e TILE_CACHE_AUTH_TOKEN=change-me \
  -e TILE_CACHE_ADMIN_PASSWORD=change-this-password \
  -v tile-cache-data:/app/tiledata \
  -v tile-cache-config:/app/config \
  harbor.cangling.cn:22002/cangling/tile-cache:latest
```

镜像使用固定 UID `10001` 运行。Docker named volume 会由 Docker 管理；使用宿主机
bind mount 时，需要预先让目录对 UID 10001 可写，例如：

```bash
mkdir -p ./tiledata ./config
sudo chown -R 10001:10001 ./tiledata ./config
```

镜像内置 `HEALTHCHECK`，通过 `tile-cache healthcheck` 访问本实例 `/health`。

重要环境变量：

| 变量 | 缺省值 | 说明 |
| --- | --- | --- |
| `TILE_CACHE_ADDR` | `0.0.0.0:7601` | 监听地址 |
| `TILE_CACHE_WORKER_THREADS` | `4` | Tokio 异步运行时工作线程数；高并发实例可按压测结果增大 |
| `TILE_CACHE_ROOT` | `./tiledata` | 当前工作目录下的瓦片数据库根目录；Docker Volume 为 `/app/tiledata` |
| `TILE_CACHE_CONFIG_DIR` | `./config` | 当前实例的管理数据库目录；Docker Volume 为 `/app/config`，多实例不得共享 |
| `TILE_CACHE_AUTH_TOKEN` | 空 | PUT/DELETE/管理 API 的 Bearer Token；生产启动必须设置，GET 始终公开 |
| `TILE_CACHE_ALLOW_UNAUTHENTICATED_WRITES` | `false` | 仅隔离开发环境使用；显式设为 `true` 才允许无 Token 启动 |
| `TILE_CACHE_SECURE_COOKIES` | `false` | 通过 HTTPS 反向代理提供 Dashboard 时设为 `true`，为会话 Cookie 增加 `Secure` |
| `TILE_CACHE_ADMIN_PASSWORD` | 空 | Dashboard 管理员初始密码；仅在管理员不存在时使用，未设置则随机生成 |
| `TILE_CACHE_MAX_TILE_BYTES` | `33554432` | 单瓦片最大字节数 |
| `TILE_CACHE_MEMORY_CACHE_BYTES` | `536870912` | 进程内瓦片 LRU 容量（字节），即 512 MiB；`0` 禁用 |
| `TILE_CACHE_MEMORY_CACHE_MAX_TILE_BYTES` | `2097152` | 允许进入 LRU 的单瓦片最大字节数，即 2 MiB |
| `TILE_CACHE_WRITE_QUEUE` | `4096` | 有界写队列容量 |
| `TILE_CACHE_READ_CONNECTIONS` | `4` | 每个已打开分片的 SQLite 最大读连接数 |
| `TILE_CACHE_SHARD_IDLE_SECONDS` | `300` | 分片无请求后关闭连接的时间；每分钟检查一次，`0` 禁用 |
| `TILE_CACHE_CLEANUP_HOUR` | `4` | 每日自动清理时间（Asia/Shanghai，0–23） |
| `TILE_CACHE_RETENTION_DAYS` | `7` | 自动清理天数，`0` 禁用 |

### 备份与恢复

- `tiledata/` 是瓦片主数据，应纳入持久卷备份。
- `config/tile-cache-meta.db` 保存显示名称、回收保护、访问统计、清理历史和 Dashboard
  管理员信息；每个实例必须使用独立 `config/`，多实例不可共享该 SQLite。
- 一致性备份应先停止容器，再同时备份 `tiledata/` 与 `config/`。恢复时保持原目录结构
  和 UID 10001 的写权限。
- 如果只有 `tiledata/`，服务可重建数据库和图层目录；启动后登录 Dashboard 执行
  “重建目录”，或调用 `POST /api/v1/admin/catalog/rebuild`。人工显示名称、回收保护、
  访问统计和清理历史无法从瓦片分片恢复。
- 升级前保留上一版本镜像和两个目录的快照；回滚时必须同时恢复与旧版本对应的
  `config/` 快照，避免管理库结构跨版本不兼容。

## 发布

发布流程参考 `cangling-broker`：只在推送 `v*` 标签时运行 GitHub Actions，分别在原生 amd64 和 arm64 runner 上测试、编译，然后构建多架构镜像并推送到 Docker Hub 和 Harbor，同时上传两个架构的镜像压缩包。

```bash
./release.sh
```

仓库需要配置：`DOCKERHUB_USERNAME`、`DOCKERHUB_TOKEN`、`HARBOR_USERNAME`、`HARBOR_PASSWORD`、`CANGLING_TOKEN`、`SOFTWARE_TOKEN`。
