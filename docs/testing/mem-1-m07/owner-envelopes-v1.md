# M07 profile v1 的物件包络与退出合同

本页与 profile-v1.json 记录 approved revision 6 的实施目标。自有对象在增长前取得有限能力；第三方 transport 使用公开配置与库外准入，字节只作结构上界并待测量。具体实施与验证状态见 coverage 和 evidence，不从目标算术推出产品完成。本页不是 RSS 上限、MEM 账本、默认硬件容量或测试通过证明。Linux 正式测试由用户后续手动执行；本地旧路径的错误/拒绝样本是基线事实，不从分母删除。

## FE 共存

| owner | 位置 × 每位置完整物件 | 增长前接点 / 共存 | 最后释放 oracle |
|---|---|---|---|
| client window | 320 × 8MiB | 放行事务取得整个窗口；两个最多2MiB的receive/prost backing、两个最多1MiB compact body；schema 1MiB、metadata 512KiB、staging/coalescing/cursor与终态均在8MiB内。原+新copy共存先覆盖 | kernel移除可见性后仍跟踪所有receive/prost/compact/write aliases；短fetch/body/job实际退出 |
| closing | 64 × 8MiB | 取消cut只非阻塞取得一次。覆盖上述原始backing、完整当前行尾、冻结schema/metadata、writer/header/ERR/flush；不得只按slice长度；8MiB不足立即disconnect | writer/flush/socket/body最后holder实际退出；计算permit归还不归还此位置 |
| local | 16 × 96MiB | snapshot16 + raw page1 + decoded page4 + collector32 + workspace32 + window8 =93MiB；先取得，再开始list/Arrow/build/copy | SDK访问、转码/collector作业join以及全部事实/Arrow别名退出 |
| internal domain | 4 × 1GiB | 最多两个336MiB facts sets + transform256 + input32 + assembly32 + bookkeeping8 + window8 =1008MiB。第三个阶段先释放旧事实或明确拒绝 | child/转换/commit observation/collector与各事实别名真实退出 |
| ordinary connection | 512 × 2MiB | command1MiB；live/staged/scalar-child/assignment/view各128KiB；auth64KiB；有限framing/registry余量。单value64KiB、vars/views各64；所有旧API也检查 | statement generation结束与实际protocol writer/socket退出；不能把KILL接受当退出 |
| control connection | 32 × 2MiB | 独立connection/input/framing界；不借ordinary已耗尽的位置 | 实际command/write/socket结束 |
| control owner | 128MiB | 有限control operation/observation/诊断队列，所有独立clone/快照计入；payload生成前取得 | RPC/status/诊断作业与最后clone退出 |
| waiters | 64MiB | ordinary最多4096个payload-free waiter，每个4KiB；连接command已归connection owner；有限其他identity/generation/counter metadata | 准入handoff、取消出队与实际持有者退出 |
| Native 自有 nonroot workspace | 768MiB | producer/encode/decode 的有限自有对象目标，增长前检查；不能以第三方 stream receipt 代替自有 backing 退出 | codec/job 与最后 payload alias 退出 |
| Native 第三方 transport | E_native_transport | 公开配置 × 数量 + 待测 c_*；详见下节。不是独立字节钱包 | connection IO wrapper、body EOF/RST/Drop、DNS closure 返回等公开事件；差额用测量门验证 |

自有对象目标小计10752MiB =10.5GiB，另加 E_native_transport。revision 6 不沿用 v5 的 Native 3GiB 和整体16GiB目标；完整传输系数尚未测量，不能宣称总包络已冻结或成立。其他FE planner/cache/catalog/allocator以及内核socket内存不属于这个小计；仍需另外部署容量，不能把它称为进程RSS保证。所有尺寸按actual capacity、真实backing identity和独立copy计算。共享backing不因slice缩小，移交不代表free。

## Native 物理承载

按 [第 6 版结构算术](transport-envelope-v1.md) 与 JSON 复算，Native 不再承诺逐连接全部内部
backing≤2MiB，也不通过第三方 vendor 接缝逐字节授权。FE outgoing live/dial/closing 总608个，
另含96个 incoming Membership连接及独立32个bootstrap位置（FE总handshake160）；各连接最多128 stream位置，公开 receive window、
header list 与服务端 send buffer 有限。客户端 send buffer使用批准的hyper默认1MiB/stream。

`E_native_transport = Σ N_conn × [conn_window + N_stream × (stream_window + send_buf + header_list)]
+ N_conn×c_conn + N_stream_total×c_stream + N_handshake×c_handshake + N_queue×c_queue`。
当前FE结构项103232MiB（100.8125GiB），加自有目标为111.3125GiB且还不含c_*；这是保守配置
结构算术，不是驻留预测或默认硬件需求。c_*与线性/回落测量仍未冻结/执行，不能用空系数当零。

nonroot producer/encode/decode各64个全局真实位置的自有对象目标保留：producer展开≤4MiB，
encode实际backing≤2MiB，decode wire≤2MiB且展开≤4MiB；合计768MiB。
自有payload/copy/clone仍先取得能力，最后alias退出才归还。第三方连接/stream在库外限制数量，
公开退出事件归还位置；DNS阻塞closure实际返回前保留permit；测量门验证事件后内部析构差额。

P00b还必须冻结真实lane grid、系数、轮次、容差和当前main旧路径基线，P09进行测量门。
不得把现有Cargo测试、配置算术或已实现计数准入当成传输测量通过；P08仍依赖全生产保护闭合。

同一BE最多2个合法FE，准确身份槽等旧holder全部退出才复用。BE对每peer有Exchange2+1dial+1closing、filter1+1+1，实际peer数P=B−1，最多31。data与control独立listener、handshake(32/8)、关闭、FD及推进位置；control不等待data握手位置。FE FD保守608 Native outbound +544 client +160 Native inbound/handshake/close +4 listener +64 files +256余量=1636，要求limit≥2048；BE按准确topology计算并要求≥1024。TLS与防火墙必须覆盖两个准确端点。

## BE root 与同一 retained 防护

root共享一个输入位置和一个编码cursor，最多64个准确driver。最后一次upstream pull之前取得root资格及整个合法共存能力；其他DOP driver不再生成一个待投root输入再等待。96MiB原input +96MiB新增hydrate +2MiB scratch +64KiB staging +3×(S+4096) queued/active segments +2×(S+4096) ACK后仍由send alias持有的retired segments +4×(S+4096)独立send copies +1MiB schema/cursor/driver metadata +有限End，约204.1MiB，小于256MiB joint root cap。原batch slice不缩小backing；完整输入identity/capacity必须preflight。dictionary合法且可准确游标读取；必要hydrate先检查展开、取得能力并有限推进，不先generic整批cast再补检查。

64MiB指完整MysqlText row payload，含cell length prefix。不构造完整encoded row；大值仍需驻留原input，因此当前16MiB root默认不支持该最大行。row合法不代表所有nested/dictionary backing都合法；同时须满足输入96MiB、新增hydrate96MiB、scratch/depth/elements界。超过任一界准确 originating failure。

同现有ResultRetainedBudget设置有限profile：root256MiB、process4GiB。共享transport类与urgent control保底先由同process防护覆盖，root不能耗尽保底；不是新encoder钱包。process aggregate暂不足时在生成最后输入之前等待/可取消，单项超界则错误。该cap不保证640root同时生产最坏输入；集中roots的协议位置与实际生产内存是不同承诺。后续M03a/M03b安装真实allocation归属/自身上限，M04b接窗口事前授权；M07不假称已计费。

## 尾部与绝对期限

| 尾部 | 停止发布 / 最大合法工作 | 退出 deadline 与 oracle |
|---|---|---|
| root count/render/hydrate | 取消立即撤下一turn；每turn≤64KiB emit/1024cells，depth≤64，root最多一个cursor | 2s；作业join、input/scratch/backing最后alias=0。调度必须保证每合法turn有限CPU；阻塞第三方改有限算法或拒绝 |
| fetch/ACK/body/codec | cut封新fetch；每root只一个RPC、最多两send holders，message/connection真实cap | fetch1s、connect2s、短尾2s；取消I/O并join、EOF/RST、last backing，超时不消账 |
| closing row/metadata/ERR | cut只有已校验且驻留完整当前尾才移交；不fetch、不render、不写后续行，不ACK丢suffix整段 | 固定5s；writer/flush与所有alias实际退出；满池/缺尾/覆盖不足立即disconnect |
| SDK/list/local/domain | 先取得page/collector/workspace，禁止全量SDK Vec后检查；page≤256entries、raw1MiB、decoded4MiB、pages1024、token4096；拒绝不前进token | absolute10s/page2s；明确取消并join底层任务，最后快照/事实释放，滴流不续期 |
| accept/auth/command | connection/handshake先取得位置；auth64KiB、command1MiB、prepared long-data亦消费界；制止无限续包/sequence错 | handshake2s、auth/command10s；实际socket/read任务退出，分域保留control推进 |
| Native cache/observation/dial/close | 封新generation工作，停止生成frame，有限queue所有payload继续保留owner；peer SETTINGS不扩大profile | connect/handshake2s、短tail2s；实际body EOF/RST、codec/job/connection join与last alias |

取消支持burst32、持续16/s，在short-tail退出2s前提下H=32+ceil(16×2)=64；K=C256+H64=320。超过该范围回到原有有界payload-free准入队列。绝对deadline到期但holder仍活着时保留责任与位置，报告健康故障并限制新放行，不假归还。
