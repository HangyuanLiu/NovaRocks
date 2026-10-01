# M07 profile v1 的物件包络与退出合同

本页与 profile-v1.json 是 P00 冻结的实施目标。全部数值是 allocation 前要落实的上限；目前产品没有实施这些新限制。本页不是 RSS 上限、MEM 账本、默认硬件容量或测试通过证明。Linux 正式测试由用户后续手动执行；本地旧路径的错误/拒绝样本是基线事实，不从分母删除。

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
| Native附加 | 3GiB | 下节2724.5MiB，剩余为固定owner/cache/close metadata，禁止另藏大payload | 每exact process/lane/generation的queue/body/stream/connection/codec最后holder退出 |

上述共存小计13056MiB =12.75GiB，小于此profile声明的16GiB结果相关物件包络。其他FE planner/cache/catalog/allocator以及内核socket内存不属于这个小计；仍需另外部署容量，不能把它称为进程RSS保证。所有尺寸按actual capacity、真实backing identity和独立copy计算。共享backing不因slice缩小，移交不代表free。

## Native 物理承载

每FE对全部32BE：normal/dial/closing分别为352/128/128，总608connections。每connection全部内部独立backing最多2MiB；dial阶段禁止携payload stream。各lane为 Result4+1+1、Observation4+1+1、Submission2+1+1、Control1+1+1。cache以process+endpoint+lane+generation定位，single-flight；超旧generation还未退出时仍计同cap。

FE root stream总量320，不能再乘BE数。Observation最多32×(320+32+32)=12288，Submission和Control分别32×64=2048，总16704。持续订阅空闲只持bookkeeping/idle scratch；frame生成需要全局真实holder。每stream最多4KiB bookkeeping、8KiB idle decoder、两份16KiB raw/expanded header。保守tonic pending为608×8×4KiB metadata；payload继续持原owner能力，不深clone成另一个队列。

nonroot producer/encode/decode各64个全局真实位置；producer全部展开≤4MiB，encode actual backing≤2MiB，decode actual wire≤2MiB并且展开≤4MiB；logical wire≤1MiB+4KiB，不压缩。必须在生成、读取声明长度、reserve、protobuf decode/encode、copy/clone前取得；等待时不归还尚未消费的receive credit、不另排body。大型submit/status事实只有准确既有分段合同或明确拒绝，不能先构造再超限。preflight按元素/深度/Vec与String capacity限制展开，不以wire大小乘常数代替。

checked小计为1216MiB connections +768MiB nonroot +65.25MiB bookkeeping +130.5MiB idle +522MiB headers +19MiB pending +3.75MiB root请求 =2724.5MiB。root receive/compact以及closing另归前节，不能再次分配未计费carrier。

2MiB connection界必须包含真实H2 frame/read/write slabs、TLS、HPACK、control/reset状态；1MiB H2 receive credit本身不是这个证明。P04/P05须在Native owner以及必要的vendor transport接缝限制actual buffer capacity、frame/holder数量和partial header/zero/tiny DATA；仅设frame16KiB、header16KiB、reset32不足。若任一库内部增长不能由准确owner限制，禁止P08切换，继续修owner；不将未证明的2MiB当已实施事实。

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
