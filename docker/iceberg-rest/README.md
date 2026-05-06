# Local Iceberg REST Catalog

这个目录提供一套面向 NovaRocks REST Catalog 验证的本地环境：

- `tabulario/iceberg-rest:1.6.0`：Iceberg REST Catalog 服务，监听 `127.0.0.1:8181`。
- `quay.io/minio/minio`：S3-compatible object store，监听 `127.0.0.1:9000`，控制台为 `127.0.0.1:9001`。
- `quay.io/minio/mc`：启动时创建 `warehouse` 和 `novarocks` bucket。

## 启动

```bash
docker compose -f docker/iceberg-rest/docker-compose.yml up -d
```

如果本机是从镜像站下载的 `tabulario/iceberg-rest:1.6.0`，先补本地 tag，让 Compose 不再访问 Docker Hub：

```bash
docker tag docker.1panel.live/tabulario/iceberg-rest:1.6.0 tabulario/iceberg-rest:1.6.0
```

验证 REST 服务：

```bash
curl -s http://127.0.0.1:8181/v1/config
```

验证 MinIO：

```bash
docker compose -f docker/iceberg-rest/docker-compose.yml ps
```

MinIO 控制台：

- URL: http://127.0.0.1:9001
- user: `admin`
- password: `admin123`

## NovaRocks Catalog SQL

启动 standalone server 后，可以用下面的 catalog 指向这套 REST 服务：

```sql
CREATE EXTERNAL CATALOG ice_rest
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "http://127.0.0.1:8181",
  "warehouse" = "s3://warehouse/",
  "aws.s3.endpoint" = "http://127.0.0.1:9000",
  "aws.s3.access_key" = "admin",
  "aws.s3.secret_key" = "admin123",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
```

`uri` 是 REST Catalog endpoint；`warehouse` 和 `aws.s3.*` 是 NovaRocks 本地读取 Iceberg metadata/data files 时需要的 object-store 配置。

## 清理

停止服务：

```bash
docker compose -f docker/iceberg-rest/docker-compose.yml down
```

同时删除 MinIO 数据：

```bash
docker compose -f docker/iceberg-rest/docker-compose.yml down -v
```

## 当前 NovaRocks 覆盖边界

当前代码已经能解析 `iceberg.catalog.type = rest` 并通过 `RestCatalogBuilder` 做 `/v1/config` 握手。实际 SQL DDL/加载路径仍有部分 helper 直接走 Hadoop/S3 metadata 发现逻辑；用这套环境做端到端验证时，需要确认测试点确实走到了 REST catalog dispatcher，而不是只验证了同一个 S3 warehouse 上的 Hadoop-style 路径。
