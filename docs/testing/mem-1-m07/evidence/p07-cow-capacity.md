# P07 admitted COW backing

DML's exact COW match entry checks the original runtime binding and complete Internal maximum before layout/collector construction. It acquires no second window. Owned native decode, signed cast copies and final selection preserve actual holder, including empty and schema-only streams. Whole legacy decoded carrier retains its holder; its raw Arrow aliases still rely on transitional protection, which remains until P08.

Validation: focused COW 26 PASS, including wrong Local class refusal and last selection clone / last Arrow column alias exit, empty selection. FE full 1452 PASS, including the previous catalog materialization failure. This is not workspace/native/SQL/performance acceptance and does not activate Root wire output.

Logs:
- `logs/mem-1-m07/p07-cow-capacity.log` SHA256 `b49b83e2fe52dc1ee2070abfecc1bf32b0a899b4c6cd0904c1a13a7e84842c61`
- `logs/mem-1-m07/p07-cow-capacity-final.log` SHA256 `3f9b8dfa1dac511d366b8216917645fe30fd272ac4f46a95b28703b534bbd13f`
- `logs/mem-1-m07/p07-cow-capacity-frontend-full.log` SHA256 `bacb17f46c62f766efb2da92abb15ee13b2dbc8c87fe1a88463e6807fadd7085`
