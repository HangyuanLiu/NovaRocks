# v5 Native probe manifest 归档

revision 6 已撤回 v5 的第三方 transport 补丁。这里的六个旧 probe manifest 指向历史 b92b worktree 的 patched vendor graph，不能当作当前 Cargo 工作区。2026-10-07 将它们改名为 `Cargo.toml.reference`；文件内容逐字保持，原路径→归档路径与 SHA256 见 `retired-v5-probe-manifests.json`。旧 source pins、锁文件、代码和运行日志保留为历史证据；旧检查点的成功不能外推为 revision 6 验收。

当前 DataSketches 源码检查规则未改，仍验证全部活动工作区。历史 probe 要在对应旧版本/补丁环境重建后才能复现；当前版本不恢复这些 vendor 补丁。
