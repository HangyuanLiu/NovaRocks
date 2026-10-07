# P07 connection admission and absolute IO deadlines checkpoint

The production listener reserves one of 512 ordinary or 32 control positions
before spawning a connection task, disconnect watcher or protocol intermediary.
Exhaustion closes the accepted socket. Authentication occupies that same position.
The registration and watcher share its lifetime; the position is returned only
when the last socket-owning alias exits. Listener drain waits for that actual exit.

Control connections use normal authentication and KILL authorization. Their SQL
input is bounded at 16 KiB and the entire batch must contain only KILL commands
or empty statements before any command executes. Ordinary SQL, including the
opensrv max-packet shortcut, is refused. The refusal is flushed and then closes
the intermediary; a stalled refusal flush also closes it at the write deadline.
Prepared statements and database initialization are refused on this class.
SHOW PROCESSLIST is not admitted through this reserved path because its existing
materialized response does not fit the control diagnostic limit.

Authentication has one absolute 10 s deadline starting at connection admission,
including greeting, plugin negotiation, session admission and final flush.
A command gets one absolute 10 s deadline starting at its first received byte;
idle established connections do not start that clock. Continuation packets and
dripping input never renew it. Completion checks reject already expired but
immediately ready IO. Partial input timeout closes IO without a protocol ERR.

Legacy response construction and writes share one absolute 30 s deadline through
the final socket flush; progress does not renew it. Timeout poisons partial IO.
Relayed active writes and detached closing responses retain their separately
owned 30 s and 5 s deadlines from the earlier P07 framing checkpoint.

Validation on 2026-10-07:

- Vendored opensrv input/framing modules: 44 isolated tests passed with production
  default features disabled. Tests cover reserved refusal followed by another
  command, blocked refusal flush, slow-drip input, expired ready IO and one
  shared response write/flush deadline.
- MySQL Adapter: 64 library tests passed. Query Application: 504 library tests
  passed. Log: `logs/mem-1-m07/p07-connection-admission-20261007.log`.
- Vendor log: `logs/mem-1-m07/p07-opensrv-input-deadlines-20261007.log`.

This checkpoint is targeted evidence. Configurable connection capacities,
production root-purpose integration, native 1FE+3BE socket scenarios, full
workspace validation and P09 measurement gates remain open. It does not prove
TLS feature coverage or overall M07 completion.
