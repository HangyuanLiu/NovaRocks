# 有界结果交付测试资产

本目录维护结果协议与外部列表测试使用的固定输入、独立预期和可运行工具。
测试实现与场景入口见 [System Test Runner](../../../tests/system-test-runner/README.md)。
第三方库边界见 [ADR-0170](../../adr/ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md)。

## 固定输入

- [profile-v1.json](profile-v1.json)：结果窗口、协议、领域与 SDK 列表参数的冻结值。
  Server、SPI 与 Result Contract 的测试直接读取它；其中历史状态字段不能作为当前验收进度。
  冻结时的说明路径仅作历史来源；当前工具与保存边界以本页为入口。
- [inputs/](inputs/)：System Test Runner 使用的 wire、retention、RootResult 拒绝、ACK/replay、
  Closing 和 REST 列表输入。保留被现行输入引用的旧版本，以便核对来源与准确 SHA；
  旧版本的存在不表示继续运行其已被替代的场景。
- [result_delivery_wire_oracle.py](oracles/result_delivery_wire_oracle.py)：
  从固定字面量独立推导行字节、包数、列名和 MySQL 类型，不从引擎输出录制预期。

这些输入按原字节保留。修改 SQL、oracle、容量或期限时，应先核对场景合同，不能通过改预期掩盖失败。

## 本地工具

以下检查只复算目标算术或核对独立预期，不证明产品容量、物理释放或 Native 验收：
工具需要 Python 3.10 或更新版本；本地验证使用 Python 3.11。

```bash
python3 docs/testing/mem-1-m07/scripts/check_profile.py
python3 docs/testing/mem-1-m07/scripts/transport_envelope.py
python3 docs/testing/mem-1-m07/oracles/result_delivery_wire_oracle.py
```

传输报告中的 `coefficients` 与 `total_bytes` 为 `null`，表示测量尚未冻结，不能按零计算。
公开配置推导的结构项不能作为进程 RSS 或可部署容量承诺。

真实 REST 列表场景直接调用下面两个工具；执行绑定、原始观测及输出均写入 Git 外：

- [prepare_real_rest_cl.py](../../../tests/system-test-runner/tools/prepare_real_rest_cl.py)：核对显式冻结绑定，准备、独立核验和清理私有数据。
- [observe_real_rest_cl.py](scripts/observe_real_rest_cl.py)：按绑定启动透明观测器，保留实际请求与退出事实。

工具的本地自检可以独立运行，不启动真实 REST/HMS fixture：

```bash
python3 tests/system-test-runner/tools/prepare_real_rest_cl.py --self-test
python3 docs/testing/mem-1-m07/scripts/observe_real_rest_cl.py --self-test
```

模板必须绑定实际源码、脚本哈希和 fixture publication 后才能执行；未绑定模板不能当作 READY。
真实 fixture 使用仓库版本化 runtime owner，端点从实际 publication 读取。

## 验证与产物

生产验收使用原生 **1FE+3BE**。工具自检、组件测试和 all-in-one smoke 各自只有其实际覆盖范围。
第一批实现与已有检查的范围见 [PR #1173](https://github.com/NovaRocks/NovaRocks/pull/1173)；
剩余 SQL、原生 1FE+3BE 与最终全量验收由用户后续单独执行。传输系数、性能/内存测量及大规模只读 HMS 验证另行开展，
不作为当前功能实现 PR 的前置条件。Linux 验收由用户手动执行。

运行日志、逐检查点收据、临时探针、一次性基线/预期生成器、执行绑定和测量输出统一放在
忽略目录 `logs/mem-1-m07/`，不作为仓库长期测试资产维护。PR 正文记录精简的验证结论及其准确版本；
需要共享原始产物时，使用独立 artifact 存储。历史日志的本地路径不能当作可下载证据，
历史通过结果也不能代替当前源码的验收。
