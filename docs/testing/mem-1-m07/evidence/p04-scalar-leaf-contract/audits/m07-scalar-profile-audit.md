# ScalarValueV1 profile and neutral-owner audit

Read-only on current e61696e1f-derived tree; no Cargo or repository edits.

## Approved limits and current implementation

- Approved spec §5.6 (2026-09-28-mem-1-m07-fe-byte-quota-exit-design.md:248-252): exactly typed internal scalar; zero rows NULL, one row value, second row rejected; binary/Decimal/time precision cannot be restored from client strings. Session live/staged and conversion coexistence are original admission obligations; only whole execution success commits variables.
- Frozen P00 profile-v1.json:52-61: session_live=131072, session_staged=131072, scalar_child=131072, assignment_scratch=131072, variables=64, single_value=65536; full connection objects=2097152 at51. owner-envelopes-v1.md:13 repeats these actual-owner obligations.
- These session constants currently exist in the profile artifacts, NOT an executable SessionProfile/ScalarProfile in mysql-adapter/query-application/result-contract (searched all three). RootProfileV1 currently has client-row/schema/render bounds (result-contract/root.rs:87+), not scalar constants. Do not use row64MiB or render64KiB small-row threshold as scalar authority.
- result-contract/root.rs:25 declares ScalarValueV1 domain. FrozenRootOutput::InternalFacts currently contains only domain identity (147), with no scalar source/type descriptor. NativeRootResultSession::try_open148 still rejects every non-Statistics internal domain. Thus current scalar codec/source-purpose/BE producer/FE collector installation is OPEN.
- frontend-application/query.rs:1329 clones live SessionSqlState to staged before assignments;1367 clones staged again for each child;1387 inserts resulting SQL String;1390-1394 seals execution-success visibility before replacing live. Preserve atomic all-assignments behavior and old+new maximum coexistence.
- evaluate_governed_scalar_query1420-1493 still prepares ordinary query then consumes ExecutionOutput::Rows, not the ScalarValueV1 purpose. consume_governed_scalar_stream2608 requires exactly1 column,2655-2663 rejects second row across batches,2687-2689 publishes zero-row null only on successful End. Immediate path1458 uses the same old generic conversion.
- SessionSqlState session.rs:45 stores BTreeMap<String,String>;121-122 lowercases name + inserts with no capacity guard. substitution132-135 copies BTreeMap state and reparses SELECT+stored SQL String. This represents SQL expressions, not typed scalar ownership.

## Binary64KiB is not an escaped String backing allowance

- user_variable.rs:94-108 converts Binary/LargeBinary by String::from_utf8_lossy(...).into_owned: invalid bytes are lost; this path MUST retire for P06/P07, not be copied into SCV1.
- Existing quoted conversion178-187 first allocates escaped String (capacity raw.len()+2), appends two bytes for each quote/backslash (may realloc), then format allocates a second final String while escaped remains live.
- For raw n=65536 quote/backslash bytes, escaped logical len=131072 and final literal len=131074 (>128KiB by2). Their simultaneous logical backing is at least262146, before original input, geometric Vec/String capacity or record/schema overhead. Checked actual capacity admission must happen before escaping/allocation; String.len alone is insufficient.
- For n=65536 invalid0xff bytes, UTF8 replacement path produces196608 UTF8 bytes even before SQL quoting. This is neither exact binary nor bounded by assignment128KiB.
- A future binary-to-SQL hex expression also expands2n plus syntax; that is a separate application conversion policy, not the binary codec and not justification to raise the frozen value cap. Typed session values can retain raw bytes; parser/session consumer conversion is a later application-owner integration gate.
- 64KiB describes one value allowance; framing/type metadata and original carrier/capacity must separately fit the128KiB child/assignment guard. The spec/profile does not yet declare an SCV1 magic/tag table, wire header length or timezone/tree metadata limit. Freeze those implementation details against existing owner envelopes, rather than treating65536 as an automatically valid complete record length.

## Exact current Native source types

plan-codec/physical_type.rs:349-378 is authoritative for the existing Native output types; client renderer's larger vocabulary is NOT authority.

- Null, Boolean; signed Int8/16/32/64; Float32/64 (preserve width and IEEE raw bits at codec layer).
- LARGEINT: FixedSizeBinary(16), signed128 original bit pattern.
- Decimal128: coefficient16B + explicit precision1..38, scale0..precision. Decimal256: coefficient32B + precision1..76, scale0..precision. physical_type381-392 and native_type396-412 both reject negative scale; Arrow's standalone helper allows negative scales, so its broad acceptance must NOT widen frozen Native support. Decimal32/64 Native compatibility descriptors decode to Arrow Decimal128 (native_type389-392); freeze actual source Arrow width/metadata, no guessed declared downgrade.
- Date32 signed days since epoch unchanged; preserve zero-date sentinel raw days too. No chrono format/range fallback in binary codec.
- Time64 Microsecond raw signedi64; preserve negative/time precision. Timestamp Microsecond or Nanosecond raw signedi64 + exact optional timezone bytes; nonempty zone rule physical_type366-370. Seconds/milliseconds, Time32/Time64ns, Date64 are not existing exact Native output support. No UTC conversion, micros truncation, Local clock or chrono materialization required by binary codec.
- Utf8 raw UTF8 bytes; Binary arbitrary bytes; LargeBinary carries Variant in current Native type vocabulary (physical_type82-85); logical Json/opaque metadata must be explicit, not inferred from carrier/name. Container List/Map/Struct are already reachable one-cell assignments; scalar means cardinality1, not necessarily primitive-only. Existing native shape validation canonicalizes map children etc. Do not silently reject all containers or infer Json/opaque only from Arrow carrier.
- UInt/LargeUtf8/LargeList/FixedSizeList and broader renderer Time units are not exact Native TypeDesc support. Any broader carrier normalization needs explicit protected preparation support; not a codec fallback.

## Old generic conversion precisely loses types

- sql/literal.rs1181-1193: Decimal128 scale0 narrows to i64/rejects >i64; nonzero scale becomes Literal::String, losing Decimal typed identity. Decimal256 has no extraction case (1293 catchall).
- Float32 widens to f64 at1172; scalar codec should preserve original32bits. User-variable Literal rendering rejects nonfinite floats142-148; that is existing SET application policy, not a reason for binarycodec to normalize bits or assume a new SQL behavior.
- Timestampµs1229-1240 formats whole seconds, discards timezone/fraction, and expect may panic outside chrono range. Timestampns/Time64 have no old extractor case. Binary generic literal1207/1214 is Latin1 mapping; user-variable path uses lossyUTF8 instead. Both are text representations, not exact typed scalar transport.
- List/Struct/Map literal1243-1291 allocate complete recursive Vec/Literal trees; user_variable151-173 builds Vec<String> + join + format. Their current reachability must inform the frozen type matrix, while new pure cursor/collector must bound depth/elements/bytes and avoid unbounded tree/materialization.

## Smallest proper ownership boundary

1. novarocks-result-contract (currently dependency-free; Cargo.toml) should own frozen ScalarType/ScalarSchema + scalar profile constants, immutable wire grammar, typed borrowed value views, header/type/length validation and pure count/emit/decode cursors where they only consume caller-owned bytes. It already owns root purpose/wire vocabulary; never put Arrow arrays, runtime guards, sockets, SQL Literal or MySQL formatting here.
2. novarocks-type-contract already depends on result-contract + arrow-schema: an exact Arrow DataType <-> frozen ScalarType check/bridge belongs here if it needs Arrow schema. Logical Json/Variant/opaque classification is supplied explicitly by trusted plan/field owner; no field-name guess or render presentation.
3. Native Adapter owns Arrow extraction/source cursor -> pure codec plus actual RootInputAuthority/pool/scratch/cancel/exit. Follow root_statistics_codec ownership pattern but share scalar grammar with FE, rather than duplicating a BE-only decoder contract. Freeze scalar purpose/type/source occurrence BEFORE dispatch; root contract must carry more than domain identity to bind exact source type.
4. Query Application owns FE finite collector and typed session takeover; Frontend Application prepares/finalizes exact scalar purpose and commits successful SET. mysql-adapter owns protocol budgets/input/metadata, not a second session scalar representation.
5. result-render is the ClientRows/MySQL presentation owner and is not the neutral ScalarValueV1 owner; do not emulate MySQL render bytes or reuse ClientRenderSchema presentations for private binary scalar facts.

## Focused independent future oracles

- Binary exact [00,ff,80,quote,backslash], payload65535/65536/65537 with no UTF8/text conversion; separate literal expansion131074 boundary fail before new String capacity. Actual allocator/Weak backing old+new/staged/live ownership, not just outputlen.
- Decimal128 extrema beyondi64, Decimal25676-digit and declaredprecision mismatch; timestampµ/ns +/-1 rawunits plus timezone, Time64 negative, Date sentinel and out-of-chrono-range rawdays. Independent expected LE bytes/bit equality; no comparison only against old lossy helper.
- Zero/multi-empty batches->one NULL only after success End; one value across segments; second row in same/next batch rejects and cancels actual producer; End truncation, badtag/length/depth/metadata fail before collector allocation. Child128KiB + staged/live coexistence and scope exit remain distinct from packet ACK/close.
