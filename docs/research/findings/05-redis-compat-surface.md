# Redis compatibility surface for a "basic Redis alternative" (RESP2/RESP3 wire format, pipelining, minimal command set, reply and error formats, Kvrocks/Dragonfly gaps, and compatibility test tooling) as of 2026-09-26

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:redis-compat-surface`, agent `a6dfa8d28cb099785`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Implement one RESP codec with a per-connection protocol version (default 2, switched by HELLO 3) and route every reply through typed helpers (null, double, map, set, verbatim) so the same command code emits '$-1'/'*-1' vs '_', flat arrays vs '%'/'~' aggregates, bulk-string vs ',' doubles. go-redis defaults to HELLO 3, so RESP3 is not optional if Go clients are a target; redis-rs and redis-cli default to RESP2, so RESP2 must stay byte-exact.
2. Implement HELLO [2|3] [AUTH u p] [SETNAME n] returning the seven documented fields (server, version, proto, id, mode, role, modules) and '-NOPROTO' for other versions; report a Redis-style version string (Kvrocks reports 7.0.0) because clients parse it. Also implement CLIENT SETNAME/GETNAME/ID/INFO/LIST and accept CLIENT SETINFO LIB-NAME/LIB-VER (ignore or store), since go-redis and redis-rs send them on every connection.
3. Parser must accept both multibulk and inline requests, enforce Redis' limits (64 KiB inline, INT_MAX multibulk count, proto-max-bulk-len 512 MiB default, pre-auth caps of 10 args / 16 KiB bulk) and emit the exact 'Protocol error: ...' strings followed by connection close; tests/unit/protocol.tcl and Kvrocks' protocol_test.go check these strings verbatim. Treat an empty inline line and '*0' as no-ops.
4. Per-connection command execution must be strictly FIFO with replies queued in order (pipelining); a natural design is one reader goroutine/task per connection feeding a serial executor, with write batching, so 10k-command pipelines from redis-benchmark -P and redis-cli --pipe (which relies on a trailing ECHO to detect completion) work.
5. Error replies must reuse Redis' fixed strings: build a shared error table (WRONGTYPE, 'ERR syntax error', 'ERR no such key', 'ERR index out of range', 'ERR value is not an integer or out of range', 'ERR value is not a valid float', 'ERR increment or decrement would overflow', 'ERR increment would produce NaN or Infinity', 'ERR wrong number of arguments for '<cmd>' command' with lowercase fullname, 'ERR unknown command '<cmd>', with args beginning with: ...', 'NOAUTH Authentication required.', 'EXECABORT Transaction discarded because of previous errors.'), and strip CR/LF from user-derived text before embedding it in a simple error.
6. Transactions: keep MULTI state per connection; validate arity/existence at queue time (reply the error immediately and mark the transaction dirty), reply '+QUEUED' otherwise; on EXEC return '-EXECABORT ...' if dirty, a null array if a WATCHed key changed (including expiry or FLUSHDB/FLUSHALL), else an array of per-command replies where runtime errors are elements; always UNWATCH on EXEC/DISCARD/disconnect; reject nested MULTI, WATCH inside MULTI, EXEC/DISCARD without MULTI with the exact strings. Map WATCH onto a per-key version counter or RocksDB sequence number check at EXEC and run EXEC as one RocksDB WriteBatch so it is atomic and isolated from other connections.
7. Expiry must be stored as absolute Unix milliseconds, checked lazily on every key access (return nil/none/-2 as if deleted) and swept actively (a background scan or a RocksDB compaction filter); SET without KEEPTTL, GETSET, DEL and *STORE clear it, INCR/LPUSH/HSET/ZADD keep it, RENAME transfers it; implement EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT with NX/XX/GT/LT and TTL/PTTL/PERSIST/EXPIRETIME.
8. SCAN over RocksDB should encode the cursor as an unsigned 64-bit number derived from the last key seen (e.g. a hash or an opaque offset into a metadata column family) and return '0' at the end; apply MATCH/TYPE after retrieval, honour COUNT as a hint, allow returning zero keys with a non-zero cursor, and reply 'ERR invalid cursor' for non-numeric cursors. This maps naturally onto a RocksDB iterator Seek and is the piece the Kvrocks/Dragonfly designs differ on (Kvrocks' DBSIZE is asynchronous), so document NilDB's DBSIZE cost explicitly.
9. Sorted sets and geo can share one encoding: a member->score column family plus a (score,member)->'' index CF with scores encoded as order-preserving 8-byte doubles; GEOADD is then ZADD with the 52-bit interleaved geohash as score, GEOSEARCH/GEORADIUS become 9 score-range iterator scans plus a haversine filter using Earth radius 6372797.560856 m, GEODIST formats with 4 decimals, GEOPOS decodes from the hash, GEOHASH re-encodes to 11 base32 chars. This gives Redis geo semantics for free and the same CF can back the MongoDB-style $near/$geoWithin work planned elsewhere. Note Kvrocks documents that its BYLEX ranges ignore scores; NilDB should follow Redis (lex order only meaningful within equal scores) or document the deviation.
10. Ship a minimal but exact command surface first (PING, ECHO, QUIT, SELECT, AUTH, HELLO, CLIENT *, COMMAND (+COUNT/INFO/DOCS returning at least an empty/basic table), INFO with '# Server', '# Clients', '# Memory', '# Keyspace' sections and 'dbN:keys=..,expires=..,avg_ttl=..' lines, DBSIZE, FLUSHDB/FLUSHALL [SYNC|ASYNC], KEYS, SCAN, TYPE, EXISTS, DEL, EXPIRE family, strings, hashes, lists, sets, zsets, geo, MULTI/EXEC/DISCARD/WATCH/UNWATCH, CONFIG GET save/appendonly). redis-benchmark only warns when CONFIG GET fails, redis-cli falls back from COMMAND DOCS to COMMAND to a static table, so partial CONFIG/COMMAND support is acceptable; but every listed command's reply type must match the doc for RESP2 and RESP3.
11. Follow Kvrocks' and Dragonfly's precedent of a published 'supported commands' table with explicit x-marks and footnotes (e.g. no OBJECT ENCODING, no CLIENT TRACKING, no Lua at first, whether SELECT is real). Dragonfly's warning that 'Fully supported does not imply byte-for-byte identical behavior' is the honest framing NilDB should also use.
12. Compatibility testing strategy: (1) unit tests at the raw-socket level modelled on protocol.tcl/Kvrocks protocol_test.go; (2) run the upstream Redis TCL suite against NilDB with './runtest --host 127.0.0.1 --port <p> --singledb --ignore-encoding --ignore-digest --tags -needs:debug --tags -needs:repl --tags -needs:save --single unit/type/string' etc. and record pass/fail per unit; (3) drive redis-cli (interactive, --pipe, --scan, --bigkeys, -3) and redis-benchmark (default tests plus -P 16 and -3) in CI; (4) a Go integration suite using go-redis v9 (mirroring Kvrocks' tests/gocase structure: util.StartServer spawning the binary with a config, testify assertions) and, if Rust is chosen, an equivalent using redis-rs with both protocol=resp2 and resp3. Note that on macOS Apple Silicon this needs Homebrew redis (for redis-cli/redis-benchmark/runtest) and tcl.
13. Language choice signal from this topic alone: both go-redis (used by Kvrocks' own tests, RESP3-by-default) and redis-rs (RESP2 default, explicit HELLO 3 opt-in) are mature; the Redis TCL suite and redis-cli/redis-benchmark are language-neutral. Go gets a ready-made integration-test template from Kvrocks; Rust gets stronger typing for the codec. The decision should therefore be driven by the RocksDB-binding research, not by client compatibility.
14. Keep the 'native RocksDB feel' from leaking into wire behaviour: RocksDB snapshots/iterators are fine for implementing SCAN, KEYS, LRANGE and GEOSEARCH consistently, but replies and errors must still look like Redis; expose RocksDB-specific operations (checkpoints, compaction, column-family stats) as separate namespaced commands (e.g. 'NIL.CHECKPOINT', like Kvrocks' COMPACT/STATS/DISK USAGE) rather than by changing standard command semantics.

## Open questions

- Exact WRONGPASS/AUTH error texts and the QUIT/ECHO/GETRANGE/APPEND reply pages were not opened in this session; confirm against the command reference before freezing the error table.
- Which Redis version string should NilDB report in HELLO/INFO (Kvrocks reports 7.0.0)? Some clients gate features on it (go-redis maintenance notifications, client-side caching); a too-new version may cause clients to send commands NilDB lacks.
- Will NilDB support multiple databases (SELECT n) as separate RocksDB column-family prefixes, or follow Kvrocks' original placeholder approach? This affects the TCL suite (--singledb) and MOVE/SWAPDB/FLUSHALL semantics.
- How will WATCH be implemented over RocksDB: per-key version counters in a metadata CF, RocksDB sequence numbers, or an in-memory watch table? Each has different behaviour under active expiry and FLUSHDB (multi.tcl checks both).
- Which SCAN cursor encoding gives Redis' guarantees (no misses for stable keys) on RocksDB when keys are inserted between calls, and how will DBSIZE be computed cheaply (Kvrocks makes it asynchronous)?
- Should NilDB implement RESP3 push/attribute types (needed only for CLIENT TRACKING and pub/sub over RESP3) in the first cut, or stop at the request-response subset that redis-cli, go-redis and redis-rs need?
- How much of COMMAND / COMMAND DOCS metadata must be populated for cluster-aware clients and redis-cli hints; is a static JSON table generated from Redis' commands/*.json acceptable?
- Dragonfly claims about MULTI/EXEC isolation are 'fully supported' but not byte-identical; NilDB needs to decide whether EXEC batches are serialised globally (single writer) or per shard, and document the resulting isolation level.

## Findings (46)

### 1. RESP is a CRLF-delimited, type-prefixed protocol; RESP2 has 5 types (+ simple string, - error, : integer, $ bulk string, * array) and RESP3 adds _ null, # boolean, , double, ( big number, ! bulk error, = verbatim string, % map, | attribute, ~ set, > push.

The protocol spec table lists each type with its first byte and minimal protocol version. Examples given byte-exact: '+OK\r\n', ':1000\r\n', '$5\r\nhello\r\n', '$0\r\n\r\n' (empty string), '*0\r\n', '*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n', '_\r\n', '#t\r\n', ',1.23\r\n', ',inf\r\n', ',-inf\r\n', ',nan\r\n', '(3492890328409238509324850943850943825024385\r\n', '!21\r\nSYNTAX invalid syntax\r\n', '=15\r\ntxt:Some string\r\n' (3-byte encoding + ':' + data), '%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n', '~<n>\r\n...', '><n>\r\n...'. Integers are signed 64-bit. Simple strings must not contain CR or LF. Redis' own RESP3 support excludes streamed strings/aggregates ('$?', '*?' ... '.\r\n'); Redis never emits them.

- Source: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Confidence: verified; design-critical: yes

### 2. RESP2 null is encoded as '$-1\r\n' (null bulk string) or '*-1\r\n' (null array); RESP3 uses a single '_\r\n'. The server picks the encoding by the connection's negotiated protocol version.

Spec: 'Null bulk strings ... $-1\r\n', 'Null arrays ... *-1\r\n' (e.g. BLPOP timeout), RESP3 'Nulls ... _\r\n'. Redis source server.c createSharedObjects: shared.null[2]='$-1\r\n', shared.null[3]='_\r\n', shared.nullarray[2]='*-1\r\n', shared.nullarray[3]='_\r\n', shared.emptymap[2]='*0\r\n', shared.emptymap[3]='%0\r\n'. multi.c EXEC uses shared.nullarray[c->resp] when a WATCH failed.

- Source: https://github.com/redis/redis/blob/unstable/src/server.c
- Confidence: verified; design-critical: yes

### 3. Clients send commands as a RESP array of bulk strings only; the server may reply with any RESP type. Command names are case-insensitive.

Spec 'Sending commands to a Redis server': 'A client sends the Redis server an array consisting of only bulk strings'; example 'LLEN mylist' is sent as '*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n' and answered ':48293\r\n'. COMMAND doc: 'Redis command names are case-insensitive.'

- Source: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Confidence: verified; design-critical: yes

### 4. Inline commands: any request whose first byte is not '*' is parsed as a space-separated line (with quoting), terminated by LF or CRLF; inline requests are capped at 64 KiB (PROTO_INLINE_MAX_SIZE) and quoting errors produce '-ERR Protocol error: unbalanced quotes in request'.

Spec 'Inline commands': 'Since no command starts with *, Redis detects this condition and parses your command inline' (example 'C: PING' / 'S: +PONG'). server.h: '#define PROTO_INLINE_MAX_SIZE (1024*64)'. networking.c processInlineBuffer searches for '\n', strips a preceding '\r', splits with sdssplitargs; error replies 'Protocol error: too big inline request' and 'Protocol error: unbalanced quotes in request'. tests/unit/protocol.tcl asserts '*too big inline request*', '*unbalanced*', and 'Handle an empty query' (a bare CRLF is ignored).

- Source: https://github.com/redis/redis/blob/unstable/src/networking.c
- Confidence: verified; design-critical: yes

### 5. Multibulk parsing limits and exact protocol error strings: multibulk count must be <= INT_MAX; bulk length must be >= 0 and <= proto-max-bulk-len (default 512 MiB, min 1 MiB); before AUTH, count > 10 or bulk length > 16384 is rejected; a length line longer than 64 KiB without CR is rejected; the byte after a count must be '$'.

networking.c processMultibulkBuffer: '!ok || ll > INT_MAX' -> 'Protocol error: invalid multibulk length'; 'll > 10 && authRequired(c)' -> 'Protocol error: unauthenticated multibulk length'; 'c->querybuf[c->qb_pos] != $' -> "Protocol error: expected '$', got '%c'"; '!ok || ll < 0 || ll > server.proto_max_bulk_len' -> 'Protocol error: invalid bulk length'; 'll > 16384 && authRequired(c)' -> 'Protocol error: unauthenticated bulk length'; over-long count strings -> 'Protocol error: too big mbulk count string' / 'too big bulk count string'. After a protocol error Redis closes the connection (setProtocolError). config.c: proto-max-bulk-len default 512ll*1024*1024, minimum 1024*1024; client-query-buffer-limit default 1 GiB. tests/unit/protocol.tcl asserts these exact substrings.

- Source: https://github.com/redis/redis/blob/unstable/src/networking.c
- Confidence: verified; design-critical: yes

### 6. Connections start in RESP2; HELLO <2|3> [AUTH user pass] [SETNAME name] switches protocol and returns a map (RESP2: flat array) with server, version, proto, id, mode, role, modules. Unsupported protover must yield '-NOPROTO ...'; a RESP2-only server yields '-ERR unknown command' and clients fall back.

HELLO doc (since 6.0.0): 'connections start in RESP2 mode'; example RESP2 reply is a 14-element flat array (server redis, version 8.8.0, proto 2, id, mode standalone, role master, modules ...) and RESP3 reply is a %-map. Spec 'Client handshake': mandatory fields server, version, proto; 'HELLO 4' -> '-NOPROTO sorry, this protocol version is not supported.'; 'HELLO 3' on old server -> '-ERR unknown command HELLO'; HELLO with bad AUTH returns error and stays RESP2. Source: server.h default c->resp = 2; config.c 'client-default-resp' hidden config default 2.

- Source: https://redis.io/docs/latest/commands/hello/
- Confidence: verified; design-critical: yes

### 7. RESP3 changes reply shapes for several core commands: HGETALL returns a map (%), SMEMBERS a set (~), ZRANGE ... WITHSCORES / ZSCAN / ZRANDMEMBER WITHSCORES return an array of [member, double] pairs instead of a flat alternating array, ZSCORE/ZINCRBY/ZADD INCR return doubles (,) instead of bulk strings, and INFO returns a verbatim string (=txt:).

HGETALL doc: RESP2 'Array reply ... flat list', RESP3 'Map reply'. SMEMBERS doc: RESP2 Array, RESP3 Set. ZINCRBY doc: RESP2 'Bulk string reply', RESP3 'Double reply'. ZADD doc INCR: RESP2 bulk string, RESP3 double. INFO doc: RESP2 Bulk string, RESP3 Verbatim string. Source t_zset.c: 'if (withscores && c->resp == 2)' flat length, 'if (withscores && c->resp > 2) addReplyArrayLen(c,2)' then addReplyDouble; t_hash.c uses addReplyMapLen for HGETALL; t_set.c uses addReplySetLen. networking.c addReplyDouble emits ',<d2string>' when c->resp == 3 else a bulk string. ZRANK WITHSCORE returns a 2-element array in both protocols.

- Source: https://github.com/redis/redis/blob/unstable/src/t_zset.c
- Confidence: verified; design-critical: yes

### 8. Pipelining: the server must accept further commands before earlier replies are read and must answer strictly in request order on that connection; Redis queues replies in memory and recommends clients batch around 10k commands.

Pipelining doc: 'A Request/Response server can be implemented so that it is able to process new requests even if the client hasn't already read the old responses'; ordering example Client INCR X x4 then Server 1,2,3,4; netcat example printf 'PING\r\nPING\r\nPING\r\n' -> +PONG x3; 'IMPORTANT NOTE: ... the server will be forced to queue the replies, using memory ... send them as batches ... for instance 10k commands'. Transactions doc adds the cross-connection guarantee: 'A request sent by another client will never be served in the middle of the execution of a Redis Transaction.'

- Source: https://redis.io/docs/latest/develop/using-commands/pipelining/
- Confidence: verified; design-critical: yes

### 9. Generic error strings a server must reproduce byte-for-byte (clients and the TCL suite pattern-match them): '-ERR unknown command '<cmd>', with args beginning with: 'a' 'b' ', '-ERR wrong number of arguments for '<cmd>' command', '-ERR syntax error', '-WRONGTYPE Operation against a key holding the wrong kind of value', '-ERR no such key', '-ERR index out of range', '-NOAUTH Authentication required.', '-OOM command not allowed when used memory > 'maxmemory'.', '-EXECABORT Transaction discarded because of previous errors.', '-ERR unknown subcommand '<sub>'. Try <CMD> HELP.', '-ERR invalid expire time in '<cmd>' command'.

server.c createSharedObjects defines shared.wrongtypeerr, nokeyerr ('-ERR no such key'), syntaxerr ('-ERR syntax error'), outofrangeerr ('-ERR index out of range'), noautherr, oomerr, execaborterr, busykeyerr ('-BUSYKEY Target key name already exists.'), loadingerr, roslaveerr ('-READONLY You can't write against a read only replica.'). server.c commandCheckExistence formats "unknown command '%.128s'" plus ", with args beginning with: %s" where each arg is quoted 'x ' and CR/LF are mapped to spaces; commandCheckArity: "wrong number of arguments for '%s' command" using cmd->fullname (lowercase, e.g. 'client|setname'); networking.c addReplyErrorExpireTime: "invalid expire time in '%s' command"; addReplySubcommandSyntaxError: "unknown subcommand or wrong number of arguments for '%.128s'. Try %s HELP."; commands flagged no_multi get 'Command not allowed inside a transaction'. protocol.tcl asserts '*wrong*arguments*ping*'.

- Source: https://github.com/redis/redis/blob/unstable/src/server.c
- Confidence: verified; design-critical: yes

### 10. MULTI/EXEC/DISCARD/WATCH semantics: MULTI replies +OK and every subsequent command replies +QUEUED; a command that fails arity/unknown-command checks is not queued (its error is returned immediately) and EXEC then replies -EXECABORT; a runtime error inside EXEC does not stop the other commands (EXEC returns an array containing the error element); EXEC after a modified WATCHed key returns a null array; EXEC always UNWATCHes; nested MULTI, EXEC/DISCARD without MULTI, and WATCH inside MULTI are errors with fixed strings.

Transactions doc: MULTI 'always replies with OK', 'all commands will reply with the string QUEUED', telnet example 'MULTI +OK / SET a abc +QUEUED / LPOP a +QUEUED / EXEC *2 +OK -WRONGTYPE ...', 'INCR a b c -ERR wrong number of arguments for 'incr' command' not queued, 'Starting with Redis 2.6.5 ... refuse to execute the transaction returning an error during EXEC', WATCH failure 'EXEC returns a Null reply', 'When EXEC is called, all keys are UNWATCHed', expired keys abort since 6.0.9, 'Commands within a transaction won't trigger the WATCH condition'. multi.c: 'MULTI calls can not be nested', 'DISCARD without MULTI', 'EXEC without MULTI', 'WATCH inside MULTI is not allowed'; EXEC checks CLIENT_DIRTY_EXEC -> shared.execaborterr, CLIENT_DIRTY_CAS -> shared.nullarray[c->resp]; shared.queued='+QUEUED\r\n'. tests/unit/multi.tcl asserts 'EXECABORT*', WATCH on expired keys, 'After successful EXEC key is no longer watched', 'DISCARD should UNWATCH all the keys', FLUSHDB/FLUSHALL touch watched keys.

- Source: https://redis.io/docs/latest/develop/using-commands/transactions/
- Confidence: verified; design-critical: yes

### 11. SCAN cursor semantics: reply is a 2-element array [cursor as bulk string representing an unsigned 64-bit number, array of keys]; cursor 0 starts and ends a full iteration; COUNT (default 10) is only a hint; the server may return 0 elements with a non-zero cursor; duplicates are allowed; MATCH and TYPE are filters applied after retrieval; invalid cursor -> '-ERR invalid cursor'; COUNT < 1 -> syntax error; NOVALUES only for HSCAN; SSCAN/HSCAN/ZSCAN return member lists (HSCAN and ZSCAN alternate field/value and member/score).

SCAN doc 'Return value': 'first element is a string representing an unsigned 64 bit number (the cursor)'; guarantees: full iteration returns every element present for the whole iteration, never returns elements absent for the whole iteration, may return duplicates, count per call not guaranteed and may be zero, default COUNT 10, TYPE filter 'applied after elements are retrieved', small encoded aggregates return everything in one call; termination only guaranteed for bounded collections; corrupted cursor -> undefined but never a crash. db.c: parseScanCursorOrReply uses string2ull else 'invalid cursor'; 'count < 1' -> shared.syntaxerr; unknown TYPE name currently does not error (commented 'TODO: uncomment in redis 8.0'); 'NOVALUES option can only be used in HSCAN'. shared.emptyscan = '*2\r\n$1\r\n0\r\n*0\r\n'. tests/unit/scan.tcl covers COUNT, MATCH, TYPE, expired keys, NOVALUES, 'SCAN guarantees check under write load', 'SCAN COUNT overflow'.

- Source: https://redis.io/docs/latest/commands/scan/
- Confidence: verified; design-critical: yes

### 12. SET syntax as of Redis 8.4+: SET key value [NX|XX|IFEQ v|IFNE v|IFDEQ d|IFDNE d] [GET] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL]; condition options are mutually exclusive, expiry options are mutually exclusive; reply +OK, or nil when the condition fails; with GET returns the previous value (nil if absent) and errors if the existing value is not a string; any previous TTL is discarded unless KEEPTTL.

SET doc: 'Any previous time to live associated with the key is discarded on successful SET'; options table with 'since' versions: NX/XX/EX/PX 2.6.12, KEEPTTL 6.0.0, GET/EXAT/PXAT 6.2.0, IFEQ/IFNE/IFDEQ/IFDNE 8.4.0 (IFDEQ/IFDNE use an XXH3 digest from DIGEST). Return: nil 'The key doesn't exist and XX/IFEQ/IFDEQ was specified' or 'The key exists, and NX was specified or a specified IFEQ/IFNE/IFDEQ/IFDNE condition is false'; with GET: null if the key didn't exist, else previous value. t_string.c parseExtendedStringArgumentsOrReply: option letters matched case-insensitively; any unknown/duplicate combination -> shared.syntaxerr; invalid expire -> addReplyErrorExpireTime; values larger than proto-max-bulk-len -> 'string exceeds maximum allowed size (proto-max-bulk-len)'.

- Source: https://redis.io/docs/latest/commands/set/
- Confidence: verified; design-critical: yes

### 13. String arithmetic: INCR/INCRBY/DECR/DECRBY operate on signed 64-bit integers and fail with '-ERR value is not an integer or out of range' (non-integer) or '-ERR increment or decrement would overflow'; INCRBYFLOAT returns a bulk string, errors with '-ERR value is not a valid float' or '-ERR increment would produce NaN or Infinity', formats with up to 17 significant digits, no exponent, trailing zeros stripped.

INCRBYFLOAT doc: 'If the key does not exist, it is set to 0'; errors when value or increment 'are not parsable as a double'; 'The precision of the output is fixed at 17 digits after the decimal point'; 'Trailing zeroes are always removed'; example SET mykey 5.0e3 / INCRBYFLOAT mykey 2.0e2 -> "5200"; replicated as SET. t_string.c incrDecrCommand: overflow check -> 'increment or decrement would overflow'; incrbyfloatCommand: 'increment would produce NaN or Infinity'; getLongLongFromObjectOrReply / getLongDoubleFromObjectOrReply supply the 'value is not an integer or out of range' / 'value is not a valid float' texts.

- Source: https://redis.io/docs/latest/commands/incrbyfloat/
- Confidence: verified; design-critical: no

### 14. Key expiry: EXPIRE key seconds [NX|XX|GT|LT] (options since 7.0.0) returns 1 if set, 0 otherwise; non-positive timeouts delete the key (emitting 'del' not 'expired'); TTL/PTTL return -2 for a missing key and -1 for no expiry; SET/GETSET/DEL/*STORE clear the TTL while INCR/LPUSH/HSET keep it; RENAME transfers it; expiry is stored as absolute Unix ms and enforced both passively on access and actively by sampling; accuracy 0-1 ms since 2.6.

EXPIRE doc: 'The timeout will only be cleared by commands that delete or overwrite the contents of the key, including DEL, SET, GETSET and all the *STORE commands'; 'incrementing the value of a key with INCR, pushing a new value into a list with LPUSH, or altering the field value of a hash with HSET are all operations that will leave the timeout untouched'; 'EXPIRE/PEXPIRE with a non-positive timeout ... will result in the key being deleted'; NX/XX/GT/LT definitions ('A non-volatile key is treated as an infinite TTL for the purpose of GT/LT'); CLI example shows TTL -1 after SET overwrote a key and EXPIRE ... XX returning 0; 'Since Redis 2.6 the expire error is from 0 to 1 milliseconds'; passive + active expiry description.

- Source: https://redis.io/docs/latest/commands/expire/
- Confidence: verified; design-critical: yes

### 15. TYPE returns a simple string among string, list, set, zset, hash, stream (plus module types such as vectorset) or 'none' for a missing key; the source table also has 'array' (unstable).

TYPE doc: 'The different types that can be returned are: string, list, set, zset, hash, stream, and vectorset'; return 'Simple string reply: the type of key, or none when key doesn't exist'. db.c obj_type_name[] = {"string","list","set","zset","hash",NULL(module),"stream","array",...}; getObjectTypeName returns "none" for NULL.

- Source: https://redis.io/docs/latest/commands/type/
- Confidence: verified; design-critical: no

### 16. KEYS pattern is glob-style: ? single char, * any run, [ae] set, [^e] negated set, [a-b] range, backslash escapes; reply is an array; O(N) and discouraged in production.

KEYS doc lists 'h?llo', 'h*llo', 'h[ae]llo', 'h[^e]llo', 'h[a-b]llo' and 'Use \ to escape special characters'; warning 'Don't use KEYS in your regular application code'; examples 'KEYS *name*' returning lastname, firstname. SCAN MATCH uses the same matcher (db.c: pattern exactly '*' disables matching).

- Source: https://redis.io/docs/latest/commands/keys/
- Confidence: verified; design-critical: no

### 17. SELECT index: databases are per-connection state, default 16 (config 'databases'), new connections use db 0; errors '-ERR DB index is out of range' and, in cluster mode, '-ERR SELECT is not allowed in cluster mode'; clients must re-select after reconnecting.

SELECT doc: 'New connections always use database zero'; 'the currently selected database is a property of the connection, clients should track the currently selected database and re-select it on reconnection'; cluster only supports db 0. config.c: createIntConfig("databases", ..., 16, ...). db.c selectCommand: 'SELECT is not allowed in cluster mode', 'DB index is out of range'. Kvrocks note: SELECT was a placeholder returning OK until 2.15.0 introduced 'redis-databases'.

- Source: https://redis.io/docs/latest/commands/select/
- Confidence: verified; design-critical: yes

### 18. AUTH [username] password: OK on success; single-arg form implies user 'default'; failure error is '-WRONGPASS invalid username-password pair or user is disabled.'; commands before AUTH on a password-protected server get '-NOAUTH Authentication required.'. AUTH and HELLO carry the no_auth flag so they are allowed pre-auth.

AUTH doc: 'When ACLs are used, the single argument form of the command, where only the password is specified, assumes that the implicit username is default'; command_flags include 'no_auth'; HELLO doc flags also include 'no_auth'. server.c shared.noautherr = '-NOAUTH Authentication required.\r\n'. (WRONGPASS text is the standard Redis 6+ reply; the exact string was not printed from source in this session.)

- Source: https://redis.io/docs/latest/commands/auth/
- Confidence: likely; design-critical: no

### 19. CLIENT SETNAME name replies +OK, the name may not contain spaces (it would break CLIENT LIST output), empty string clears it, CLIENT GETNAME returns the name or nil; go-redis issues CLIENT SETNAME on every connection when ClientName is set and both go-redis and redis-rs issue CLIENT SETINFO LIB-NAME / LIB-VER on connect (ignoring errors).

CLIENT SETNAME doc: 'it is not possible to use spaces in the connection name as this would violate the format of the CLIENT LIST reply'; 'remove the connection name setting it to the empty string'; 'Every new connection starts without an assigned name'. go-redis redis.go initConn: pipe.ClientSetName(ctx, c.opt.ClientName) when set; then p.ClientSetInfo(WithLibraryName), p.ClientSetInfo(WithLibraryVersion) unless DisableIdentity, and only network errors (not Redis errors) abort. redis-rs connection.rs connection_setup_pipeline: cmd CLIENT SETINFO LIB-NAME 'redis-rs' and LIB-VER with .ignore() unless skip_set_lib_name.

- Source: https://github.com/redis/go-redis/blob/master/redis.go
- Confidence: verified; design-critical: yes

### 20. go-redis v9 (master 9.23.0-beta.1) defaults to Protocol 3 and sends HELLO 3 [AUTH user pass] [SETNAME name] first on every new connection; if HELLO returns any Redis error reply (e.g. unknown command) it falls back to RESP2 and legacy AUTH, but a non-Redis error (protocol/IO) closes the connection; it then pipelines SELECT <db> (if DB>0), READONLY (if configured), CLIENT SETNAME, CLIENT TRACKING ON (if client-side cache); then CLIENT SETINFO x2.

options.go: 'if opt.Protocol < 2 { opt.Protocol = 3 }'. redis.go initConn: conn.Hello(ctx, c.opt.Protocol, username, password, c.opt.ClientName); comment 'for redis-server versions that do not support the HELLO command, RESP2 will continue to be used'; 'else if !isRedisError(initErr) { ... Transition(pool.StateClosed); return initErr }' with comment naming DragonflyDB and proxies replying different strings; else 'helloFallbackToRESP2 = c.opt.Protocol == 3' and legacy Auth/AuthACL; then Pipelined Select/ReadOnly/ClientSetName/ClientTrackingOn; then ClientSetInfo pipeline. version.go: '9.23.0-beta.1'.

- Source: https://github.com/redis/go-redis/blob/master/redis.go
- Confidence: verified; design-critical: yes

### 21. redis-rs (main, crate version 1.7.1) defaults to RESP2 and sends AUTH (if a password is set), SELECT <db> (if db != 0), optional CLIENT TRACKING ON, and CLIENT SETINFO LIB-NAME/LIB-VER as one pipeline; with ?protocol=resp3 it sends HELLO 3 instead of AUTH and maps an error whose detail starts with 'unknown command `HELLO`' to ErrorKind::RESP3NotSupported.

connection.rs parse_protocol: 'None => ProtocolVersion::RESP2'; connection_setup_pipeline: 'if connection_info.protocol.supports_resp3() { pipeline.add_command(resp3_hello(...)) } else if let Some(password) ... authenticate_cmd'; SELECT added when db != 0; CLIENT TRACKING ON under cache-aio; CLIENT SETINFO LIB-NAME 'redis-rs' / LIB-VER env CARGO_PKG_VERSION with .ignore(); setup_connection retries without username on AuthResult::ShouldRetryWithoutUsername; get_resp3_hello_command_error matches detail.starts_with("unknown command `HELLO`"). Cargo.toml version = "1.7.1".

- Source: https://github.com/redis-rs/redis-rs/blob/main/redis/src/connection.rs
- Confidence: verified; design-critical: yes

### 22. redis-cli connect sequence is AUTH (-a/--user), then SELECT <n> (-n), then HELLO 3 (-3); --name sends CLIENT SETNAME; in interactive (tty) mode it calls COMMAND DOCS for hints/help and, if that errors, falls back to the plain COMMAND array plus a built-in static table; on reconnect it re-sends SELECT only, so MULTI state is lost ('ERR EXEC without MULTI').

redis-cli.c cliConnect: cliAuth -> cliSelect -> cliSwitchProto (redisCommand 'HELLO 3', prints 'HELLO 3 failed: %s'); cliSetClientName sends 'CLIENT SETNAME %s'; cliInitHelp: redisCommand(context, 'COMMAND DOCS'); on NULL or REDIS_REPLY_ERROR -> cliLegacyInitHelp + cliLegacyIntegrateHelp (redisCommand 'COMMAND'); called only when '(!config.eval_ldb) && isatty(fileno(stdin))' or for 'redis-cli help'. CLI doc: 'When a reconnection is performed, redis-cli automatically re-selects the last database number selected. However, all other states about the connection is lost, such as within a MULTI/EXEC transaction' with the '(error) ERR EXEC without MULTI' example; options '-2/-3', '--name', '--json (default RESP3)', '--show-pushes'.

- Source: https://github.com/redis/redis/blob/unstable/src/redis-cli.c
- Confidence: verified; design-critical: yes

### 23. redis-cli --pipe (mass insertion) streams raw RESP from stdin and terminates by sending '\r\n*2\r\n$4\r\nECHO\r\n$20\r\n<20 random chars>\r\n', counting replies until it sees that ECHO reply; a server must therefore implement ECHO and reply in order. redis-cli --scan/--bigkeys/--memkeys depend only on SCAN (+TYPE/MEMORY USAGE); --stat and INFO-based modes depend on INFO fields (keys, mem, clients).

redis-cli.c pipe mode: 'The ECHO sequence starts with a \r\n so that if there is garbage in the protocol we read from stdin, the ECHO ...' and the literal "\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n" template, '--pipe-timeout' default 30s. CLI doc: --scan 'uses the SCAN command', --bigkeys/--memkeys sample via SCAN, --stat prints keys/mem/clients/blocked/requests/connections (from INFO), --latency loops PING.

- Source: https://redis.io/docs/latest/develop/tools/cli/
- Confidence: verified; design-critical: no

### 24. redis-benchmark first sends 'CONFIG GET save' and 'CONFIG GET appendonly' (pipelined) and only aborts if the reply is a NOAUTH error; any other error just prints 'WARNING: Could not fetch server CONFIG' and continues. Each client connection prefixes AUTH (-a), CLIENT TRACKING on (--enable-tracking), SELECT <db> (--dbnum), HELLO 3 (-3). Default test list: PING_INLINE (raw 'PING\r\n'), PING_MBULK, SET key:__rand_int__, GET, INCR counter:__rand_int__, LPUSH, RPUSH, LPOP, RPOP, SADD myset element:__rand_int__, HSET myhash element:__rand_int__, SPOP, ZADD myzset <score> element:__rand_int__, ZPOPMIN, LPUSH (for LRANGE), LRANGE_100/300/500/600, MSET (10 keys), XADD mystream * myfield <data>. -P pipelines N requests, -c 50 clients, -n 100000 requests, -d 3 bytes, -r keyspace.

redis-benchmark.c fetchServerConfig: redisAppendCommand 'CONFIG GET save' / 'CONFIG GET appendonly'; on error 'goto fail'; fail: aborts (exit 1) only if '!strncmp(reply->str,"NOAUTH",6)', else returns NULL; caller prints 'WARNING: Could not fetch server CONFIG'. createClient prefix: AUTH, 'CLIENT TRACKING on', SELECT via raw '*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n', 'HELLO 3' if config.resp3. main: benchmark("PING_INLINE","PING\r\n",6), PING_MBULK, SET/GET/INCR/LPUSH/RPUSH/LPOP/RPOP/SADD/HSET/SPOP/ZADD/ZPOPMIN/LRANGE_100..600/MSET (10 keys)/XADD with __rand_int__ substituted by a 12-digit number. Docs page lists -c 50, -n 100000, -d 3, -P, -r, -t, --dbnum, --cluster (uses CLUSTER NODES/CLUSTER SLOTS).

- Source: https://github.com/redis/redis/blob/unstable/src/redis-benchmark.c
- Confidence: verified; design-critical: yes

### 25. COMMAND returns one array per command: name (lowercase), arity (negative = minimum, includes the command name), flags, first key, last key, step, ACL categories (6.0), tips (7.0), key specifications (7.0), subcommands (7.0); order is random; subcommands COMMAND COUNT/DOCS/INFO/LIST/GETKEYS exist. Cluster-aware clients and redis-cli use it; Dragonfly does not implement COMMAND DOCS/LIST/GETKEYS.

COMMAND doc: numbered list of the 10 elements; 'GET's arity is 2 ... MGET's arity is -2'; 'Command arity always includes the command's name itself'; flags list (write, readonly, denyoom, fast, no_multi, noscript, movablekeys ...); example GET entry with key spec map; 'The order of the commands in the array is random'. Dragonfly compatibility page: 'COMMAND DOCS, GETKEYS, GETKEYSANDFLAGS, LIST (Unsupported)'.

- Source: https://redis.io/docs/latest/commands/command/
- Confidence: verified; design-critical: no

### 26. INFO [section ...] returns a bulk string (RESP3: verbatim 'txt') of '# Section' headers and 'field:value' lines terminated by CRLF; arguments default/all/everything/modules; the Keyspace section line format is 'dbN:keys=X,expires=X,avg_ttl=X,subexpiry=X'; redis-cli in RESP2 mode special-cases INFO for display.

INFO doc: 'Lines can contain a section name (starting with a # character) or a property. All the properties are in the form of field:value terminated by \r\n'; 'When no parameter is provided, the default option is assumed'; 'all: Return all sections (excluding module generated ones)', 'everything: Includes all and modules'; keyspace 'dbXXX: keys=XXX,expires=XXX,avg_ttl=XXX,subexpiry=XXX'; examples start '# Server\r\nredis_version:7.4.0'. Protocol spec: 'When using RESP2, however, the redis-cli is hard-coded to look for the INFO command to ensure its correct display'.

- Source: https://redis.io/docs/latest/commands/info/
- Confidence: verified; design-critical: no

### 27. List commands: LPUSH/RPUSH return the new length; LPOP/RPOP [count] (count since 6.2.0) return nil for a missing key, a bulk string without count, an array with count; LTRIM start stop replies +OK, negative indexes count from the tail, start > end empties and deletes the key, stop past the end is clamped; LINDEX on missing key returns nil and LSET uses '-ERR no such key' / '-ERR index out of range'; LRANGE returns an array; LLEN an integer.

LPOP doc: 'When provided with the optional count argument, the reply will consist of up to count elements'; returns nil / bulk / array; example 'LPOP mylist 2' -> two elements. LTRIM doc: 'if start is larger than the end of the list (start > stop), the result will be an empty list, which causes key to be removed'; 'LPUSH mylist someelement / LTRIM mylist 0 99' capped-list pattern is O(1) average. t_list.c: lookupKeyWriteOrReply(... shared.nokeyerr) and shared.outofrangeerr in LSET; shared.syntaxerr for bad options.

- Source: https://redis.io/docs/latest/commands/lpop/
- Confidence: verified; design-critical: no

### 28. Sorted sets: ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...; scores are doubles (+inf/-inf accepted, integers exact only within ±2^53), members with equal scores order lexicographically by bytes; reply integer added (or changed with CH), or bulk-string/double new score with INCR, nil when NX/XX/GT/LT blocks; exact errors: '-ERR XX and NX options at the same time are not compatible', '-ERR GT, LT, and/or NX options at the same time are not compatible', '-ERR INCR option supports a single increment-element pair', '-ERR resulting score is not a number (NaN)', '-ERR syntax error' for odd score/member counts.

ZADD doc: 'The score values should be the string representation of a double precision floating point number. +inf and -inf values are valid'; 'NX and XX are mutually exclusive, as are GT and LT'; precision note '-(2^53) and +(2^53)'; 'When multiple elements have the same score, they are ordered lexicographically ... binary'; returns list incl. 'Nil reply: if the operation was aborted because of a conflict with one of the XX/NX/LT/GT options'. t_zset.c zaddGenericCommand: 'elements % 2 || !elements' -> syntaxerr; the three incompatibility strings; nanerr 'resulting score is not a number (NaN)'. ZINCRBY doc: creates key/member if missing; ZRANK doc: 0-based, nil if missing, WITHSCORE (7.2.0) returns [rank, score].

- Source: https://redis.io/docs/latest/commands/zadd/
- Confidence: verified; design-critical: yes

### 29. ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES] (6.2.0) subsumes ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANGEBYLEX/ZREVRANGEBYLEX; score bounds accept -inf/+inf and '(' for exclusive; lex bounds require '[' or '(' or -/+; with REV and BYSCORE/BYLEX the arguments are swapped (start is the high bound); index ranges are inclusive with negative offsets and never error; errors: '-ERR min or max is not a float', '-ERR min or max not valid string range item', '-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX', '-ERR syntax error, WITHSCORES not supported in combination with BYLEX'.

ZRANGE doc: 'ZRANGE zset (1 5 BYSCORE ... 1 < score <= 5', 'ZRANGE zset 10 5 REV BYSCORE ... scores less than 10 and greater than 5', 'Out of range indexes do not produce an error', LIMIT 'A negative <count> returns all elements from the <offset>', BYLEX 'Valid <start> and <stop> must start with ( or [', '+ or - ... positive and negative infinite strings', WITHSCORES 'value1,score1,...'. t_zset.c zrangeGenericCommand: option loop (withscores, limit, rev, bylex, byscore) else syntaxerr, then the two 'syntax error, ...' messages; zslParseRange failure 'min or max is not a float'; zslParseLexRange failure 'min or max not valid string range item'. Legacy commands (zrangebyscoreCommand etc.) still exist in source.

- Source: https://redis.io/docs/latest/commands/zrange/
- Confidence: verified; design-critical: yes

### 30. GEO data model: GEOADD key [NX|XX] [CH] lon lat member ... stores members in an ordinary sorted set whose score is a 52-bit interleaved geohash (26 steps per axis); valid longitude -180..180, latitude -85.05112878..85.05112878 (EPSG:3785 limits); out-of-range -> '-ERR invalid longitude,latitude pair %f,%f'; NX+XX or wrong arg multiple -> '-ERR syntax error'; there is no GEODEL (use ZREM); distance uses haversine with Earth radius 6372797.560856 m (up to 0.5% error).

GEOADD doc: 'Latitude and Longitude bits are interleaved to form a unique 52-bit integer'; limits 'Valid longitudes are from -180 to 180 degrees. Valid latitudes are from -85.05112878 to 85.05112878 degrees'; 'there is no GEODEL command because you can use ZREM'; Haversine 'error may be up to 0.5%'. geohash.h: GEO_STEP_MAX 26, GEO_LAT_MIN -85.05112878, GEO_LAT_MAX 85.05112878, GEO_LONG_MIN -180, GEO_LONG_MAX 180. geohash_helper.c: EARTH_RADIUS_IN_METERS = 6372797.560856; geohashGetDistance = 2*R*asin(sqrt(a)). geo.c: 'invalid longitude,latitude pair %f,%f'; '(c->argc - longidx) % 3 || (xx && nx)' -> syntaxerr; 'unsupported unit provided. please use M, KM, FT, MI'; 'radius cannot be negative'. tests/unit/geo.tcl asserts '-ERR invalid longitude,latitude pair*' and uses the same 6372797.560856 haversine as reference.

- Source: https://github.com/redis/redis/blob/unstable/src/geo.c
- Confidence: verified; design-critical: yes

### 31. GEOSEARCH key <FROMMEMBER m | FROMLONLAT lon lat> <BYRADIUS r unit | BYBOX w h unit> [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH] (6.2.0) replaces deprecated GEORADIUS/GEORADIUSBYMEMBER; reply is an array of member names, or with any WITH* an array of [member, dist?, hash?, [lon, lat]?] in that fixed order; distances are formatted with 4 decimals; exact errors: 'exactly one of FROMMEMBER or FROMLONLAT can be specified for <cmd>', 'exactly one of BYRADIUS and BYBOX can be specified for <cmd>', 'the ANY argument requires COUNT argument', 'COUNT must be > 0', 'STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options' / 'GEOSEARCHSTORE is not compatible with ...', 'could not decode requested zset member'.

GEOSEARCH doc: syntax and 'This command should be used in place of the deprecated GEORADIUS and GEORADIUSBYMEMBER'; reply 'Array reply of arrays ... 1. distance ... 2. Geohash integer 3. coordinates as a two items x,y array'; example shows '"Catania" "56.4413" ["15.087267458438873" "37.50266842333162"]'. GEORADIUS doc: RESP example '["Palermo","190.4424",["13.361389338970184","38.115556395496299"]]', STORE/STOREDIST, GEORADIUS_RO variants, 'Deprecated as of Redis v6.2.0'. geo.c: addReplyDoubleDistance uses fixedpoint_d2string(...,4); option parser with the quoted error strings; duplicate FROM*/BY* -> syntaxerr (geo.tcl asserts 'ERR *syntax*', 'ERR *exactly one of FROMMEMBER or FROMLONLAT*', 'ERR *ANY*requires*COUNT*').

- Source: https://redis.io/docs/latest/commands/geosearch/
- Confidence: verified; design-critical: yes

### 32. GEOPOS returns an array of [lon, lat] bulk-string pairs (nil element per missing member) with values decoded from the 52-bit hash, so they differ slightly from the input; GEODIST member1 member2 [M|KM|FT|MI] returns a bulk string with 4 decimals or nil if either member is missing (default unit meters); GEOHASH returns 11-character standard geohash strings (the 11th char is always computed from zero bits) or nil per missing member.

GEOPOS doc: 'the coordinates returned may not be exactly the ones used ... small errors may be introduced'; example 'GEOPOS Sicily Palermo Catania NonExisting' -> two pairs and '(nil)'. GEODIST doc: 'If one or both the members are missing, the command returns NULL'; 'defaults to meters'; examples "166274.1516", "166.2742" km, "103.3182" mi. GEOHASH doc: 'returns 11 character Geohash strings'; example "sqc8b49rny0". geo.c geohashCommand: re-encodes with lat range -90..90 / lon -180..180 at 26 steps, 'if (i == 10) idx = 0' comment 'We have just 52 bits, but the API used to output an 11 bytes geohash'.

- Source: https://redis.io/docs/latest/commands/geopos/
- Confidence: verified; design-critical: yes

### 33. Hash commands: HSET accepts multiple field/value pairs (4.0.0) and returns the count of new fields; HGETALL returns a flat array in RESP2 (empty array for a missing key) and a map in RESP3; HINCRBY/HINCRBYFLOAT error with '-ERR hash value is not an integer' / '-ERR value is NaN or Infinity' / 'increment or decrement would overflow'.

HGETALL doc: 'every field name is followed by its value, so the length of the reply is twice the size of the hash'; RESP2 'Array reply ... or an empty list when key does not exist', RESP3 'Map reply'. t_hash.c: 'hash value is not an integer', 'increment or decrement would overflow', 'value is NaN or Infinity'; HSET arity check via addReplyErrorArity (odd argument count -> wrong number of arguments).

- Source: https://redis.io/docs/latest/commands/hgetall/
- Confidence: verified; design-critical: no

### 34. Set commands: SADD/SREM return counts; SISMEMBER returns 1/0; SMEMBERS returns an array (RESP2) / set (RESP3) in unspecified order; SPOP/SRANDMEMBER accept a count; SINTER/SUNION/SDIFF and their *STORE variants exist; SINTERCARD numkeys errors with 'Number of keys can't be greater than number of args'.

SMEMBERS doc: RESP2 'Array reply', RESP3 'Set reply'; 'same effect as running SINTER with one argument'. Protocol spec: 'SISMEMBER returns 1 for true and 0 for false'; 'SADD, SREM ... return 1 when the data changes and 0 otherwise'. t_set.c: 'Number of keys can't be greater than number of args' and shared.syntaxerr for bad LIMIT.

- Source: https://redis.io/docs/latest/commands/smembers/
- Confidence: verified; design-critical: no

### 35. Apache Kvrocks (RocksDB-backed, 'compatible with Redis protocol') documents as unsupported: BRPOPLPUSH; OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT; SCRIPT KILL/DEBUG; CONFIG RESETSTAT; CLIENT CACHING/GETREDIR/NO-EVICT/NO-TOUCH/TRACKING/TRACKINGINFO/UNBLOCK. Everything in strings/hashes/lists/sets/zsets/geo/keys/transactions is listed as supported, with caveats: SET KEEPTTL and GET only since v2.8.0; WATCH/UNWATCH since v2.4.0; HELLO since v2.2.0; SELECT was a no-op placeholder until 2.15.0 ('redis-databases' config, incompatible with namespaces); DBSIZE and INFO keyspace are updated asynchronously only after 'DBSIZE SCAN'; DEBUG only supports SLEEP; lexicographical zset ranges ignore scores; LINDEX and LTRIM are O(N); SRANDMEMBER returns the first N members; bitmap and string are distinct types.

kvrocks-website docs/supported-commands.md rows marked 'x' or '𐄂': BRPOPLPUSH (line 75), OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT, SCRIPT KILL/DEBUG, CONFIG RESETSTAT, CLIENT CACHING/GETREDIR/NO-EVICT/NO-TOUCH/TRACKING/TRACKINGINFO/UNBLOCK. Notes quoted: 'SET ... (supported KEEPTTL and GET options since v2.8.0)'; 'SELECT ... Switches between databases when redis-databases > 0 (default 0: returns OK without switching) since 2.15.0'; ':::note The response of DBSIZE and keyspace section of INFO is updated asynchronously after executing DBSIZE SCAN command. Before 2.15.0 ... the SELECT command is just a placeholder ... we don't allow using the namespace feature and the Redis database at the same time'; 'DEBUG ... (Only DEBUG SLEEP is supported.)'; ZRANGE/ZRANGEBYLEX 'Lexicographical order in kvrocks disregards scores; this may differ from Redis behavior when members have varying scores (see discussion #3487)'; 'LINDEX ... (O(N) operation, do not use it when the list is too long)'; 'SRANDMEMBER ... (always first N members if not changed)'; 'String and bitmap are different types in Kvrocks'. README: 'uses RocksDB as storage engine and is compatible with Redis protocol'.

- Source: https://kvrocks.apache.org/docs/supported-commands
- Confidence: verified; design-critical: yes

### 36. Dragonfly's compatibility page (verified against Dragonfly v2.0.0 and Redis 8.6.4) marks MULTI/EXEC/WATCH, SELECT, all geo, hash, list, set, sorted-set and pub/sub commands 'Fully supported' but warns 'Fully supported does not imply byte-for-byte identical behavior'; unsupported or partial: SET (missing IFEQ/IFNE/IFDEQ/IFDNE), LCS, MIGRATE, SWAPDB, OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT, WAITAOF, COPY DB, CLIENT GETREDIR/NO-EVICT/NO-TOUCH/REPLY/TRACKINGINFO/UNBLOCK, CLIENT TRACKING (no BCAST/PREFIX/REDIRECT), COMMAND DOCS/GETKEYS/GETKEYSANDFLAGS/LIST, FUNCTION */FCALL, SCRIPT DEBUG/KILL, FLUSHALL/FLUSHDB ASYNC, BGREWRITEAOF, FAILOVER, LATENCY *, MEMORY DOCTOR/PURGE, MODULE *, SHUTDOWN ABORT, BITOP ANDOR/DIFF/ONE, several XADD/XTRIM/XSETID/XREADGROUP options, most CLUSTER subcommands. Its README says it has implemented '~185 Redis commands (roughly equivalent to Redis 5.0 API)'.

Page header 'Verification: Dragonfly v2.0.0; Redis 8.6.4; modules: BF, CF, CMS, FT, JSON, TDIGEST, TOPK, TS.' and the disclaimer quoted; per-section Unsupported/Partially lists as enumerated (String: 'SET (Partially): Missing: IFDEQ, IFDNE, IFEQ, IFNE', 'LCS (Unsupported)'; Generic: MIGRATE, OBJECT *, WAITAOF, COPY 'Missing: DB'; Connection: CLIENT GETREDIR/NO-EVICT/NO-TOUCH/REPLY/TRACKINGINFO/UNBLOCK, CLIENT KILL 'Missing: MAXAGE, SKIPME, USER', CLIENT TRACKING 'Missing: BCAST, PREFIX, REDIRECT'; Server: COMMAND DOCS/GETKEYS/GETKEYSANDFLAGS/LIST, SWAPDB, FLUSHALL/FLUSHDB 'Missing: ASYNC', SHUTDOWN 'Missing: ABORT', MEMORY USAGE 'SAMPLES ... accepted but ignored'). README line: 'We have to date implemented ~185 Redis commands (roughly equivalent to Redis 5.0 API) and 13 Memcached commands.'

- Source: https://www.dragonflydb.io/docs/command-reference/compatibility
- Confidence: verified; design-critical: yes

### 37. The Redis TCL test suite can be pointed at any RESP server with './runtest --host <addr> --port <port>'; tests tagged external:skip are skipped automatically; '--singledb' avoids SELECT, '--ignore-encoding' skips OBJECT ENCODING checks, '--ignore-digest' skips DEBUG DIGEST checks, '--tags -needs:debug' (etc.) excludes tests requiring DEBUG, CONFIG SET maxmemory, CONFIG RESETSTAT, RESET, SAVE/BGSAVE, replication; '--single unit/type/string' or '--only <name>' selects tests; '--list-tests' lists units. Relevant units: unit/protocol, unit/keyspace, unit/expire, unit/scan, unit/multi, unit/geo, unit/info-command, unit/auth, unit/quit, unit/introspection, unit/type/{string,incr,list,hash,set,zset}.

tests/README.md table (--singledb 'Only use database 0, don't assume others are supported', --ignore-encoding, --ignore-digest, --cluster-mode, --large-memory) and tag table (external:skip, needs:repl, needs:debug 'Uses the DEBUG command or other debugging focused commands (like OBJECT REFCOUNT)', needs:config-maxmemory, needs:config-resetstat, needs:reset, needs:save); example './runtest --host <host> --port <port> --tags -needs:repl'. test_helper.tcl option help strings for --single/--only/--list-tests/--tags/--host/--port/--singledb/--ignore-encoding/--ignore-digest; all_tests is built from tests/unit and tests/unit/type. GitHub API listing of tests/unit shows protocol.tcl, keyspace.tcl, expire.tcl, scan.tcl, multi.tcl, geo.tcl, info-command.tcl, auth.tcl, quit.tcl, introspection.tcl, other.tcl; tests/unit/type has string.tcl, incr.tcl, list.tcl (+list-2/3/4), hash.tcl, set.tcl, zset.tcl, stream.tcl.

- Source: https://github.com/redis/redis/blob/unstable/tests/README.md
- Confidence: verified; design-critical: yes

### 38. tests/unit/protocol.tcl exercises raw-socket edge cases a compatible server must pass: empty query, negative/out-of-range/non-numeric multibulk and bulk lengths, wrong payload header ('expected '$', got 'f''), commands split across reads, too-big bulk/mbulk count strings, too-big inline request, unbalanced quotes, 'Generic wrong number of args' (ping x y z), protocol desync regression, raw protocol responses, and (with DEBUG PROTOCOL) RESP3 attributes, big numbers, booleans, verbatim strings.

protocol.tcl test names and assertions listed: 'Handle an empty query', 'Negative multibulk length', 'Out of range multibulk length' (assert_error '*invalid multibulk length*'), 'Wrong multibulk payload header' ('*expected '$', got 'f'*'), 'Negative/Out of range/Non-number multibulk payload length' ('*invalid bulk length*'), 'Too big bulk count string', 'Too big multibulk count string', 'Too big inline request', 'Unbalanced number of quotes' ('*unbalanced*'), 'Protocol desync regression test' ('*Protocol error*'), 'RESP3 attributes' (needs:debug resp3), 'test big number parsing', 'test bool parsing', 'test verbatim str parsing', 'test large number of args'.

- Source: https://github.com/redis/redis/blob/unstable/tests/unit/protocol.tcl
- Confidence: verified; design-critical: yes

### 39. Kvrocks tests compatibility with a Go suite (tests/gocase) that uses go-redis/v9 (v9.17.2), testify, Go 1.25, and a raw TCPClient for byte-level protocol checks; the harness spawns the kvrocks binary with a generated kvrocks.conf (-binPath, -workspace flags, and -cliPath pointing at redis-cli) and is run via './x.py test go build' which executes 'go test -timeout=1800s -bench=. ./... -binPath=... -cliPath=redis-cli -workspace=...'. Unit dirs mirror the Redis TCL layout (protocol, scan, multi, expire, keyspace, geo, hello, info, type/{strings,incr,list,hash,set,zset,...}).

tests/gocase/go.mod: 'go 1.25.0', 'github.com/redis/go-redis/v9 v9.17.2', 'github.com/stretchr/testify v1.11.1', testcontainers. util/server.go: StartServerWithCLIOptions writes kvrocks.conf ('%s %s\n' per config), exec.Command(binPath), NewClient returns redis.NewClient, NewTCPClient for raw RESP; 'please set the workspace by -workspace'. x.py test_go: find_command('go'), find_command(cli_path, msg='redis-cli is required for testing'), args ['test','-timeout=1800s','-bench=.','./...','-binPath=...','-cliPath=...','-workspace=...']. GitHub API listing of tests/gocase/unit: applybatch auth client command config connection copy debug ... expire ... geo hello ... info ... keyspace ... multi ... protocol ... scan ... select server slowlog sort type wait; unit/type: bitmap bloom hash incr json list set sint stream strings tdigest timeseries zset.

- Source: https://github.com/apache/kvrocks/tree/unstable/tests/gocase
- Confidence: verified; design-critical: yes

### 40. Kvrocks' protocol_test.go shows a concrete RESP2-vs-RESP3 conformance table: with resp3-enabled=no, double -> '$5 3.141', set -> '*3', map -> flat '*6', bignum -> '$37 ...', true/false -> ':1'/':0', null -> '$-1', verbatim -> '$15 verbatim string', ZRANK WITHSCORE on a missing key -> '*-1', ZRANGE WITHSCORES -> flat '*6'; after 'HELLO 3' (server replies '%6' with server=redis, version=7.0.0, proto=3, mode=standalone, role=master, modules=_) double -> ',3.141', set -> '~3', map -> '%3', bignum -> '(...', booleans '#t'/'#f', null '_', verbatim '=19 txt:verbatim string'. It also asserts the Redis protocol error strings 'invalid multibulk length' and 'invalid bulk length'.

protocol_test.go TestProtocolRESP2 / TestProtocolRESP3 tables as quoted; handshakeWithRESP3 expects '%6','$6','server','$5','redis','$7','version','$5','7.0.0','$5','proto',':3','$4','mode','$10','standalone','$4','role','$6','master','$7','modules','_'; TestProtocolNetwork subtests 'out of range multibulk length' MustMatch 'invalid multibulk length', 'negative multibulk payload length' MustMatch 'invalid bulk length', 'inline protocol with quoted string'.

- Source: https://github.com/apache/kvrocks/blob/unstable/tests/gocase/unit/protocol/protocol_test.go
- Confidence: verified; design-critical: yes

### 41. MSET/MGET: MGET returns an array with nil elements for missing or non-string keys (never errors on type); MSET with an odd number of arguments returns the arity error; both are non-atomic-looking but executed atomically in one command.

Protocol spec 'Null elements in arrays' example '*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n' and COMMAND doc examples 'mset ... arity -3 ... step 2' and 'mget ... arity -2 ... step 1'. t_string.c msetGenericCommand uses addReplyErrorArity for '(c->argc % 2) == 0'. (MGET's nil-for-wrong-type behaviour is standard Redis and is asserted in the TCL string tests; not re-read in this session.)

- Source: https://redis.io/docs/latest/commands/command/
- Confidence: likely; design-critical: no

### 42. PING replies +PONG (or echoes its argument as a bulk string), ECHO returns its argument as a bulk string, QUIT replies +OK then the server closes the connection, DBSIZE returns an integer, FLUSHDB/FLUSHALL [SYNC|ASYNC] reply +OK (any other option -> syntax error), EXISTS returns the count of existing keys (duplicates counted), DEL returns the number of keys removed, PERSIST returns 1/0.

server.c shared.pong = '+PONG\r\n'; db.c flushdb/flushall parse 'ASYNC'/'SYNC' else shared.syntaxerr, reply shared.ok; delGenericCommand returns the deleted count; protocol spec inline example 'C: EXISTS somekey / S: :0'. (PING/ECHO/QUIT reply shapes are from the command reference pages generally; the QUIT page was not opened in this session.)

- Source: https://github.com/redis/redis/blob/unstable/src/db.c
- Confidence: likely; design-critical: no

### 43. GETRANGE/SETRANGE/APPEND/STRLEN: SETRANGE offset errors with '-ERR offset is out of range' when negative or when the result would exceed proto-max-bulk-len ('string exceeds maximum allowed size (proto-max-bulk-len)'); GETRANGE clamps negative and out-of-range indexes and returns a bulk string (empty for a missing key); APPEND returns the new length and creates the key when absent.

t_string.c: checkStringLength -> 'string exceeds maximum allowed size (proto-max-bulk-len)'; setrangeCommand: 'offset is out of range'; getrangeCommand and appendCommand present (lines 645, 1364). Behavioural details of GETRANGE clamping and APPEND semantics come from the general Redis command reference and were not re-read in this session.

- Source: https://github.com/redis/redis/blob/unstable/src/t_string.c
- Confidence: likely; design-critical: no

### 44. Current version markers to target: the Redis docs describe Redis 8.8 (HELLO example 'version 8.8.0'; redis-cli 'Starting with Redis 8.8'); the unstable source tree reports REDIS_VERSION '255.255.255'; Dragonfly verified against Redis 8.6.4; go-redis master is 9.23.0-beta.1; redis-rs main is 1.7.1; Kvrocks tests pin go-redis v9.17.2. The SET IFEQ/IFNE/IFDEQ/IFDNE and DELEX/DIGEST commands are Redis 8.4+ and not yet in Dragonfly.

HELLO doc example shows '"version" "8.8.0"'; CLI doc 'Starting with Redis 8.8, redis-cli supports word-jump navigation'; src/version.h '#define REDIS_VERSION "255.255.255"'; Dragonfly page 'Verification: Dragonfly v2.0.0; Redis 8.6.4'; go-redis version.go '9.23.0-beta.1'; redis-rs Cargo.toml 'version = "1.7.1"'; kvrocks go.mod 'github.com/redis/go-redis/v9 v9.17.2'; SET doc 'since 8.4.0' for IFEQ family; Transactions doc 'Starting with version 8.4, Redis offers new atomic compare-and-set and compare-and-delete commands'.

- Source: https://redis.io/docs/latest/commands/hello/
- Confidence: verified; design-critical: no

### 45. RESP3 attributes ('|') may precede any reply part and must be skipped/accumulated by clients; pushes ('>') may appear between, but never inside, replies; a server implementing only the request-response subset of RESP3 can omit both and remain compatible with redis-cli, go-redis and redis-rs, but client-side caching (CLIENT TRACKING) requires pushes.

Protocol spec: 'Attributes can appear anywhere before a valid part of the protocol identifying a given type'; 'Pushed data may precede or follow any of RESP's data types but never inside them'. go-redis initConn only issues CLIENT TRACKING ON when client-side caching is configured and HELLO 3 succeeded ('trackingEnabled := !helloFallbackToRESP2 && ...'); options.go: 'client-side caching requires Protocol: 3 (RESP3)'. redis-rs only adds CLIENT TRACKING under the cache-aio feature.

- Source: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Confidence: verified; design-critical: no

### 46. redis-cli renders INFO correctly in RESP3 because it is a verbatim string; in RESP2 mode redis-cli hard-codes INFO handling. Any server that sends INFO as a plain bulk string under RESP2 and as '=<len>\r\ntxt:...' under RESP3 matches Redis.

Protocol spec 'Verbatim strings': 'When using RESP3, redis-cli displays it correctly because it is sent as a Verbatim String reply (with its three bytes being "txt"). When using RESP2, however, the redis-cli is hard-coded to look for the INFO command'. INFO doc Return information: RESP2 Bulk string, RESP3 Verbatim string.

- Source: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Confidence: verified; design-critical: no
