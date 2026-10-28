# 阿里云手动 Rust CI

首版入口仅接受完整 SHA，执行原生 `test-run.py batch`；不增加过滤、重试或跳过表。它不授予正式准入、完整 E1 e2e 或 E2 服务资格。维护 owner 为 CI 维护者。

```sh
cd tools/ci
python3 -m aliyun_ci --config aliyun_ci/config.example.json validate-config
python3 -m aliyun_ci --config aliyun_ci/config.example.json plan-run --run-id 42 --now 2026-10-28T00:00:00Z --nonce example
python3 -m unittest discover -s aliyun_ci/tests -t . -v
```

示例不包含真实部署值。150 分钟工作预算以已有托管 job 约 103 分钟为起点，首次云运行后按实测调整；200 GiB 系统盘为初值。watcher 每 10 秒观察，最多等 120 秒取消，为云平台 5 分钟预告留出上传时间。这些均是配置，不是产品限制。

平台测试在本地使用纯逻辑、记录的 API 响应及本地 HTTP/短命子进程验证请求和实际退出；不调用云 API。修改平台或 workflow 时运行，由具名 CI stage 消费。真实镜像、Runner 上线、自动释放和 Rust 闭环须独立验收；回放不证明云部署完成。云资源只在维护者审阅部署、权限和预算并明确授权后创建。

## 镜像与执行边界

基础是部署地域的官方 Ubuntu 26.04 amd64 公共镜像，准确 ImageId 必须由维护者填入 `base_image_id`。镜像提供 G19 工具模式、平台 venv、引导/上传/watcher、Runner 原生依赖与只读检查定义，不提供产品 fixture、Docker、源码 checkout、target、Runner 注册或凭证。Runner 包每日从官方 release 校验后同步 OSS，启动时按准确版本与 SHA-256 下载；镜像不会随 Runner 发布自动失效。账户 HOME 是 `/var/lib/novarocks-ci/runner`，Runner runtime 是 `agent/` 子目录。

已登记的候选镜像可以直接启动。start 核对镜像身份和实例的自动释放时间后才发布 JIT；机器上的只读检查失败就结束，清理由 teardown 执行。`promote-image` 记录已有的工具检查通过事实，不再是首次启动门，也不自动修改仓库变量。

Runner UID 在服务启动前被禁止访问阿里云 IMDS；root bootstrap 和 watcher 保留访问。该规则的实际内核行为、systemd 与真实镜像仍须 Linux P05a 验证，macOS 单测不能替代。root-only JIT 在降权执行 Runner 前删除。test job 只得到本次归档的预签名写入 URL，权限只有 contents:read。

## 部署前需要落实的值

在任何计费操作前审阅配置、ROS 模板和预算：

- 账号、地域、官方基础 ImageId、VPC、候选规格/交换机、已有 OSS bucket 与部署前缀。
- GitHub App 需要本仓库 Runner 管理与组织 runner group 只读权限；生成安装 token 的 secret 仅供控制 job。
- environment `aliyun-ci` 限制受信 main 分支；runner group 仅开放本仓库、允许公开仓库、只允许 `NovaRocks/NovaRocks/.github/workflows/aliyun-ci.yml@refs/heads/main`。设置必须由维护者落实，start 会读取核实。
- 仓库变量 `ALIYUN_CI_CONFIG_JSON`、`ALIYUN_CI_ROLE_ARN`、`ALIYUN_CI_OIDC_PROVIDER_ARN`、`ALIYUN_CI_APP_ID`；App private key 存于 `ALIYUN_CI_APP_PRIVATE_KEY`。镜像验证后显式设置 `ALIYUN_CI_IMAGE_ID`。
- ROS 额外参数需要 `VpcId` 与 GitHub OIDC issuer 的实际证书指纹 `GitHubOidcFingerprint`；不猜测指纹。可复用交换机或按模板显式新建。

预算范围包括抢占实例、系统盘、公网出站流量（配置 `internet_max_bandwidth_out_mbps`，样例5Mbps、按流量计费）、自定义镜像/快照和 OSS 对象。没有按量实例回退；自动释放与 janitor 不能保证零账单。OSS 没有无条件到期删除；未解决失败和正式引用由维护者负责保留。

## 本机运维入口

以下命令只展示用法，须在各自授权和前置值落实后执行。所有命令从 `tools/ci` 运行，用安装了 hash 锁文件的 venv Python。SDK 采用标准阿里云凭证链，GitHub token 从显式环境变量 `GH_TOKEN` 读取，未记录 token 内容。

```sh
python3 -m venv /absolute/operator-venv
/absolute/operator-venv/bin/pip install --require-hashes -r aliyun_ci/requirements.lock

python3 -m aliyun_ci --config /absolute/config.json deploy render
python3 -m aliyun_ci --config /absolute/config.json deploy plan --stack-name novarocks-ci --create --parameters /absolute/ros-parameters.json
python3 -m aliyun_ci --config /absolute/config.json deploy inspect --change-set-id CHANGE_SET_ID
# 审阅 inspect 返回的 change_set 与 reviewed_digest 后应用同一变更集。
python3 -m aliyun_ci --config /absolute/config.json deploy apply --change-set-id CHANGE_SET_ID --reviewed-digest DIGEST
python3 -m aliyun_ci --config /absolute/config.json deploy check --stack-id STACK_ID --variables /absolute/repository-variables.json

python3 -m aliyun_ci --config /absolute/config.json runner-sync
# source.tar 必须来自仓库根的 git archive --format=tar 完整SHA；CLI核对准确内容。
python3 -m aliyun_ci --config /absolute/config.json --output /absolute/new/image.json image --preparation-sha FULL_SHA --source-archive /absolute/source.tar
python3 -m aliyun_ci --config /absolute/config.json --output /absolute/new/start.json start --sha FULL_SHA --image-id IMAGE_ID
python3 -m aliyun_ci --config /absolute/config.json teardown --plan /absolute/new/start.json
python3 -m aliyun_ci --config /absolute/config.json janitor
python3 -m aliyun_ci --config /absolute/config.json promote-image --image-id IMAGE_ID --summary-key DEPLOYMENT_PREFIX/runs/RUN_ID/ATTEMPT/summary.json
```

本机 `start` 自动生成 `local-UTC-随机` 身份，仅验证开机和 Runner 上线，不派发测试。首个云副作用前，非秘密 plan 输出到 stderr；有 `--output` 时也独占保存同名 `.plan.json`，即使 start 失败也能用 `teardown --plan` 找回。start 正常输出再保存完整结果；不依赖它才能按原标签回收。

临时 STS 用于预签名时，必须通过非秘密 `ALIYUN_CI_STS_NOT_AFTER` 给出实际凭证的 UTC 到期时间；未知或短于请求 deadline 时在创建实例前拒绝。工作流在请求12小时 STS 前记录保守的有效期上界，RAM role 同样配置12小时上限。SDK 标准公开模型没有到期字段，不能根据预签名参数伪造凭证有效期。长期凭证仍由标准链提供，执行机永远不获得它。

## 工作流与交接

合入 main 之后，手动入口为 Actions 的 Run workflow，或：

```sh
gh workflow run aliyun-ci.yml --ref main -f sha=FULL_SHA -f image_id=IMAGE_ID -f mode=rust
```

`start → test → teardown` 在同一 workflow 中。`mode=rust` 只执行完整原 batch，不加过滤；`probe` 用于取消/自动释放演练。只有 probe 且 start 成功核实自动释放后才允许 `drill_skip_teardown=true`；start 失败仍回收。每次 GitHub run/attempt 独立命名，不自动重试。

OSS `runs/<run>/<attempt>/` 下保留 final/interrupted 归档和 summary。teardown 先释放算力，再读取并核对归档成员摘要；summary 分别保留原生 Rust index、材料完整性、算力终态和遗留资源。缺材料照样释放，但明确记缺口。GitHub artifact 仅作方便副本；90天平台保留不能替代 OSS 的长期保留责任。janitor 每小时、Runner 同步每日，各自保留异常材料。

GitHub 原生 `queue: max` 保留等待请求（平台上限100，超出由 GitHub 拒绝）。固定 actionlint 1.7.12 尚未识别此已发布语法，上游问题 #657/#746；验证保留原始诊断后仅排除 `unexpected key "queue" for "concurrency" section`，其余错误仍失败，不补丁第三方、不退回会替换等待者的默认队列。
