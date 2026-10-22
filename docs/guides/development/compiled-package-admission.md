# Compiled package 的容量准入与来源事实

[ADR-0185](../../adr/ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) 规定唯一 compiled 静态计划路径及 FE/BE 共用冻结政策。本指南说明当前写出配方的准入方式；codec 政策一致不代表其中每一个经验数值都由协议规定。

## 专有配额与实际输入分开

容量默认宽松；只有真实必要、明确行业共识或协议规定时才另设配额。生产 writer recipe 不另设 recipe 数、输入字段数或累计 token bytes 配额。`physical_package_v2/frozen_limits.rs` 使用现有 `WriterRecipeProjectionLimits` 的表示上界作为这三个比较阈值；它们不参与分配、循环长度或来源估算。显式调用者仍可使用有限阈值，由同一 codec 执行。

每个字段身份 token 仍须准确为 32 字节；`physical_writer_recipe_v2::Model` 按实际输入累计字段数和 token bytes，保留 checked add/mul、实际 Layout、分配失败、字段身份/type/schema/NULL 校验、来源覆盖和父控制。共享的请求、共存与 work 政策不因这三项配额取消而消失；不能据此承诺无限宽表或授予运行期内存。

## 来源核算不授予容量

`source_retained_bytes` 覆盖该作者借用的原始 retained backing，并进入投影工作估算。它是有限数值输入，不是可直接置无限的 quota；将其改为 MAX 会伪造来源并可能使受检公式溢出。来源、投影请求、共存与真实 allocator/host 结果各自保留，不能互相替代。

常量池 retained backing、IPC、通用 codec、类型递归和 transport 的其他存量限制没有在本次写出修正中全域清理，也未因此获得必要性证明。常量池完整 backing 与所选单值的 payload 是不同事实；类型/布局合法性与实际实现支持边界也不能作为任意容量阈值的统称。

## 同政策角色与失败身份

plan contract revision 15 明确隔离原 writer 专有配额政策。Server 的兼容材料包含该 revision；只改变 limits 而不隔离旧政策会让能编码的发送方遇到拒绝它的接收方。FE 与 BE 使用同源 frozen 政策，不协商、不回退旧 decoder。

取消、deadline、真实资源不足保留原 typed 首因；无法表示的 Layout/算术拒绝不能改成成功。准入、冻结往返和实际 provider 写入分层验证：codec 往返通过不证明原生分布式写入、发布或清理完成。
