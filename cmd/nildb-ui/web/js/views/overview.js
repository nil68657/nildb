// Overview: server, engine, clients, keyspace, memory and disk, ops/sec,
// column families, snapshot leases, the last checkpoint, and the busiest
// commands, refreshed every five seconds while the view is open.

import {
  h, clear, api, text, items, isErr, toJS, parseInfo, parseKV, fmtInt, fmtBytes, fmtDuration,
  fmtDateTime, fmtNum, iconButton, button,
} from '../lib.js';
import { Sparkline, table, kv, toast, toastError, confirmDialog, spinner } from '../ui.js';

const REFRESH_MS = 5000;

function tile(label) {
  const value = h('div', { class: 'stat-value' }, '—');
  const sub = h('div', { class: 'stat-sub' }, '');
  const el = h('div', { class: 'stat' }, h('div', { class: 'stat-label' }, label), value, sub);
  return { el, set(v, s = '') { value.textContent = v; sub.textContent = s; } };
}

function card(title, ...extra) {
  const body = h('div', { class: 'card-body' });
  const el = h('section', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, title), ...extra), body);
  return { el, body };
}

function usageBar(used, cap) {
  const pct = cap > 0 ? Math.min(100, (used / cap) * 100) : 0;
  const fill = h('i');
  fill.style.width = `${pct}%`;
  return h('div', { class: 'stack' },
    h('div', { class: `bar${pct > 90 ? ' warn' : ''}`, role: 'meter', 'aria-valuemin': 0, 'aria-valuemax': 100, 'aria-valuenow': Math.round(pct) }, fill),
    h('span', { class: 'faint tnum' }, `${fmtBytes(used)} of ${fmtBytes(cap)} (${pct.toFixed(0)}%)`));
}

function levels(files) {
  const max = Math.max(1, ...files);
  return h('span', { class: 'levels', title: files.map((n, i) => `L${i}: ${n}`).join('  ') },
    files.map((n) => {
      const bar = h('i', { class: n ? '' : 'zero' });
      bar.style.height = `${n ? 4 + (n / max) * 14 : 2}px`;
      return bar;
    }));
}

// parseClients reads CLIENT LIST lines of key=value fields.
function parseClients(txt) {
  return String(txt).split('\n').filter(Boolean).map((line) => {
    const o = {};
    for (const part of line.split(' ')) {
      const i = part.indexOf('=');
      if (i > 0) o[part.slice(0, i)] = part.slice(i + 1);
    }
    return o;
  });
}

export function create(app) {
  const updated = h('span', { class: 'sub tnum' });
  const busy = spinner('Refreshing');
  busy.hidden = true;
  const serverLine = h('span', { class: 'sub' });
  const head = h('div', { class: 'view-head' },
    h('h1', null, 'Overview'), serverLine, h('div', { class: 'grow' }),
    busy, updated, iconButton('refresh', 'Refresh now', () => refresh()));
  const err = h('div', { class: 'notice err', hidden: true, role: 'alert' });

  const t = {
    ops: tile('Ops per second'),
    clients: tile('Clients'),
    keys: tile('Keys'),
    mem: tile('Memory'),
    disk: tile('On disk'),
    up: tile('Uptime'),
    engine: tile('Engine'),
    errs: tile('Error replies'),
  };
  const spark = new Sparkline('Operations per second, last five minutes');
  t.ops.el.append(spark.el);
  t.ops.el.classList.add('wide');

  const cCF = card('Column families');
  cCF.body.classList.add('flush');
  const cRocks = card('RocksDB');
  const cKeyspace = card('Keyspace');
  cKeyspace.body.classList.add('flush');
  const cLeases = card('Snapshot leases');
  cLeases.body.classList.add('flush');
  const cPersist = card('Persistence');
  const cAnalytics = card('Analytics');
  const cCmds = card('Busiest commands');
  cCmds.body.classList.add('flush');
  const cClients = card('Connections');
  cClients.body.classList.add('flush');
  const cServer = card('Server');

  const body = h('div', { class: 'view-body' },
    err,
    h('div', { class: 'grid stats' }, Object.values(t).map((x) => x.el)),
    cCF.el,
    h('div', { class: 'grid cols-2' }, cRocks.el, cKeyspace.el),
    h('div', { class: 'grid cols-2' }, cLeases.el, h('div', { class: 'grid' }, cPersist.el, cAnalytics.el)),
    h('div', { class: 'grid cols-2' }, cCmds.el, cClients.el),
    cServer.el);
  const el = h('section', { class: 'view view-scroll', 'aria-label': 'Overview' }, head, body);

  let timer = 0;
  let visible = false;
  let running = false;
  let offset = 0; // server clock minus browser clock, ms
  let leases = [];

  app.on('pulse', (p) => {
    const ops = p.map((s) => s.ops);
    spark.set(ops, 150);
    const last = p[p.length - 1];
    const peak = Math.max(0, ...ops);
    t.ops.set(fmtInt(last?.ops ?? 0), `peak ${fmtInt(peak)} in 5 min · ${fmtInt(last?.cmds ?? 0)} commands since start`);
    if (visible) renderLeases();
  });

  function renderLeases() {
    const now = Date.now() + offset;
    const canRelease = !app.blocked('rocks.snapshot|release');
    clear(cLeases.body, table(
      [{ label: 'Lease' }, { label: 'Seq', num: true }, { label: 'Owner' }, { label: 'Age', num: true }, { label: 'Expires in', num: true }, { label: '', cls: 'actions' }],
      leases.map((l) => [
        h('span', { class: 'mono' }, String(l.id)), fmtInt(l.seq), h('span', { class: 'mono' }, l.owner),
        fmtDuration(now - l.created_ms), fmtDuration(Math.max(0, l.expires_ms - now)),
        canRelease ? iconButton('trash', `Release lease ${l.id}`, () => release(l), 'sm') : '',
      ]),
      { empty: 'No open leases. NIL.* commands and DOC.FIND cursors take them while they run.' }));
  }

  async function release(l) {
    const ok = await confirmDialog({
      title: 'Release snapshot lease?',
      message: 'Commands that pass AT with this id will fail with "snapshot lease expired".',
      name: `lease ${l.id} · ${l.owner}`, confirm: 'Release',
    });
    if (!ok) return;
    try {
      await api.ok(['ROCKS.SNAPSHOT', 'RELEASE', String(l.id)]);
      toast(`Released lease ${l.id}`, 'ok');
      refresh();
    } catch (e) {
      toastError(e, 'Release failed');
    }
  }

  async function refresh() {
    if (running) return;
    running = true;
    busy.hidden = false;
    try {
      const [info, rinfo, cfList, snaps, clients] = await api.batch([
        ['INFO', 'default', 'commandstats'], ['ROCKS.INFO'], ['ROCKS.CF', 'LIST'], ['ROCKS.SNAPSHOT', 'LIST'], ['CLIENT', 'LIST'],
      ], { db: 0 });
      if (isErr(info)) throw new Error(info.v);
      const cfs = isErr(cfList) ? [] : items(cfList).map(text);
      const cfInfo = cfs.length ? await api.batch(cfs.map((cf) => ['ROCKS.CF', 'INFO', cf]), { db: 0 }) : [];
      const inf = parseInfo(text(info));
      app.emit('info', inf);
      render(inf, isErr(rinfo) ? {} : toJS(rinfo), cfs, cfInfo.map((v) => (isErr(v) ? {} : toJS(v))),
        isErr(snaps) ? [] : toJS(snaps), isErr(clients) ? [] : parseClients(text(clients)));
      err.hidden = true;
      updated.textContent = `updated ${new Date().toLocaleTimeString([], { hour12: false })}`;
    } catch (e) {
      err.hidden = false;
      err.textContent = `Could not read the server: ${e.message}`;
    } finally {
      running = false;
      busy.hidden = true;
    }
  }

  function render(inf, rocks, cfs, cfInfo, snaps, clients) {
    const srv = inf.server || {};
    const st = inf.stats || {};
    const mem = inf.memory || {};
    const rdb = inf.rocksdb || {};
    const per = inf.persistence || {};
    const an = inf.analytics || {};
    if (srv.server_time_usec) offset = Number(srv.server_time_usec) / 1000 - Date.now();
    const engine = srv.nildb_engine || rocks.engine || 'rocksdb';
    const engineVer = srv.nildb_engine_version || rocks.version || srv.nildb_rocksdb_version || '';
    serverLine.textContent = `Redis ${srv.redis_version || '?'} protocol · ${engine} ${engineVer} · pid ${srv.process_id || '?'} · port ${srv.tcp_port || '?'}`;

    const ks = Object.entries(inf.keyspace || {}).map(([db, v]) => ({ db, ...parseKV(v) }));
    const keys = ks.reduce((a, x) => a + Number(x.keys || 0), 0);
    const expires = ks.reduce((a, x) => a + Number(x.expires || 0), 0);
    const mine = clients.filter((c) => (c.name || '').startsWith('nildb-ui')).length;
    t.clients.set(fmtInt((inf.clients || {}).connected_clients), `${mine} from this console · ${fmtInt(st.total_connections_received)} since start`);
    t.keys.set(fmtInt(keys), `${fmtInt(expires)} with a TTL · ${ks.length} ${ks.length === 1 ? 'database' : 'databases'}`);
    t.mem.set(fmtBytes(mem.used_memory), `Go heap · RSS ${fmtBytes(mem.used_memory_rss)}`);
    const cfNum = (i, k) => Number(cfInfo[i]?.[k] || 0);
    const sst = cfs.reduce((a, _, i) => a + cfNum(i, 'rocksdb.total-sst-files-size'), 0);
    const memtables = Number(rdb.cur_size_all_mem_tables || 0);
    t.disk.set(fmtBytes(sst), `SST files · ${fmtBytes(memtables)} in memtables`);
    const up = Number(srv.uptime_in_seconds || 0);
    t.up.set(fmtDuration(up * 1000), `since ${fmtDateTime(Date.now() + offset - up * 1000)}`);
    t.engine.set(`${engine} ${engineVer}`, `${srv.nildb_go_version || ''} · ${srv.os || ''}`);
    t.errs.set(fmtInt(st.total_error_replies), `net in ${fmtBytes(st.total_net_input_bytes)} · out ${fmtBytes(st.total_net_output_bytes)}`);

    clear(cCF.body, h('div', { class: 'table-wrap' }, table(
      [{ label: 'Column family' }, { label: 'Keys (estimate)', num: true }, { label: 'SST files', num: true }, { label: 'Live data', num: true },
        { label: 'Memtables', num: true }, { label: 'Pending compaction', num: true }, { label: 'Files L0 to L6' }],
      cfs.map((cf, i) => [
        h('span', { class: 'mono' }, cf), fmtInt(cfNum(i, 'rocksdb.estimate-num-keys')), fmtBytes(cfNum(i, 'rocksdb.total-sst-files-size')),
        fmtBytes(cfNum(i, 'rocksdb.estimate-live-data-size')), fmtBytes(cfNum(i, 'rocksdb.cur-size-all-mem-tables')),
        fmtBytes(cfNum(i, 'rocksdb.estimate-pending-compaction-bytes')),
        levels(Array.from({ length: 7 }, (_, l) => cfNum(i, `rocksdb.num-files-at-level${l}`))),
      ]),
      { empty: 'ROCKS.CF LIST returned nothing' })));

    clear(cRocks.body, kv([
      ['Block cache', usageBar(Number(rdb.block_cache_usage || 0), Number(rdb.block_cache_capacity || rocks.block_cache_bytes || 0))],
      ['Analytics cache', usageBar(Number(rdb.analytics_cache_usage || 0), Number(rdb.analytics_cache_capacity || rocks.analytics_cache_bytes || 0))],
      ['Pinned in cache', fmtBytes(rdb.block_cache_pinned_usage)],
      ['Memtables', fmtBytes(memtables)],
      ['Pending compaction', fmtBytes(rdb.estimate_pending_compaction_bytes)],
      ['Running', `${fmtInt(rdb.num_running_compactions)} compactions, ${fmtInt(rdb.num_running_flushes)} flushes`],
      ['Latest sequence', fmtInt(rdb.latest_sequence_number ?? rocks.latest_seq)],
      ['Snapshots', `${fmtInt(rdb.num_snapshots)} RocksDB, ${fmtInt(rdb.nildb_leases)} leases of ${fmtInt(rdb.nildb_max_snapshots)}`],
      ['Oldest snapshot', Number(rdb.oldest_snapshot_time) ? fmtDateTime(Number(rdb.oldest_snapshot_time) * 1000) : 'none'],
      ['Rate limiter shim', rdb.nildb_rate_limiter_priority_shim],
      ['Statistics', rdb.nildb_statistics === 'yes' ? 'on (ROCKS.STATS)' : 'off, start with --rocks-stats'],
    ]));

    clear(cKeyspace.body, table(
      [{ label: 'Database' }, { label: 'Keys', num: true }, { label: 'With TTL', num: true }, { label: 'Average TTL', num: true }, { label: '' }],
      ks.map((x) => {
        const n = Number(x.db.slice(2));
        return [
          h('span', { class: 'mono' }, x.db), fmtInt(x.keys), fmtInt(x.expires), Number(x.avg_ttl) ? fmtDuration(x.avg_ttl) : '—',
          button('Browse', () => { app.setDB(n); app.go('keys'); }, { cls: 'sm ghost' }),
        ];
      }),
      { empty: 'Every database is empty' }));

    leases = snaps;
    renderLeases();

    const cpTime = Number(per.nildb_last_checkpoint_time || 0);
    clear(cPersist.body, kv([
      ['WAL', `${per.nildb_wal || 'on'}, fsync ${per.nildb_fsync || rocks.fsync || '?'}`],
      ['Data directory', rocks.dir ? h('span', { class: 'mono' }, rocks.dir) : undefined],
      ['Read-only store', rocks.readonly],
      ['Last checkpoint', cpTime ? fmtDateTime(cpTime * 1000) : 'none since this server started'],
      ['Checkpoint directory', cpTime ? h('span', { class: 'mono' }, per.nildb_last_checkpoint_dir) : undefined],
      ['Checkpoint sequence', cpTime ? fmtInt(per.nildb_last_checkpoint_seq) : undefined],
      ['Exported CF', cpTime && per.nildb_last_checkpoint_cf ? per.nildb_last_checkpoint_cf : undefined],
    ]));

    clear(cAnalytics.body, kv([
      ['Queries', `${fmtInt(an.analytics_queries)} run, ${fmtInt(an.analytics_running)} running, ${fmtInt(an.analytics_queued)} queued`],
      ['Scanned', `${fmtInt(an.analytics_rows_scanned)} rows, ${fmtBytes(an.analytics_bytes_scanned)}`],
      ['Throttled', `${fmtDuration(an.analytics_throttle_sleep_ms)} asleep, ${fmtInt(an.analytics_rejections)} refused`],
      ['Open', `${fmtInt(an.analytics_open_leases)} leases, ${fmtInt(an.analytics_open_cursors)} cursors`],
      ['Concurrency limit', an.analytics_max_concurrent],
    ]));

    const cmds = Object.entries(inf.commandstats || {}).map(([k, v]) => ({ name: k.replace(/^cmdstat_/, ''), ...parseKV(v) }))
      .sort((a, b) => Number(b.calls) - Number(a.calls)).slice(0, 12);
    clear(cCmds.body, table(
      [{ label: 'Command' }, { label: 'Calls', num: true }, { label: 'µs per call', num: true }, { label: 'p99 µs', num: true }, { label: 'Failed', num: true }],
      cmds.map((c) => [h('span', { class: 'mono' }, c.name), fmtInt(c.calls), fmtNum(c.usec_per_call), fmtNum(c.p99), fmtInt(c.failed_calls)]),
      { empty: 'No commands yet' }));

    clear(cClients.body, h('div', { class: 'table-wrap' }, table(
      [{ label: 'Id', num: true }, { label: 'Address' }, { label: 'Name' }, { label: 'db', num: true }, { label: 'RESP', num: true }, { label: 'Age', num: true }, { label: 'Idle', num: true }, { label: 'Last command' }],
      clients.map((c) => [c.id, h('span', { class: 'mono' }, c.addr), c.name || h('span', { class: 'faint' }, 'none'), c.db, c.resp,
        fmtDuration(Number(c.age) * 1000), fmtDuration(Number(c.idle) * 1000), h('span', { class: 'mono' }, c.cmd)]),
      { empty: 'CLIENT LIST is not available' })));

    clear(cServer.body, kv([
      ['Redis version', srv.redis_version],
      ['Engine', `${engine} ${engineVer}`],
      ['Go', srv.nildb_go_version],
      ['OS', srv.os],
      ['Process', `pid ${srv.process_id}, port ${srv.tcp_port}`],
      ['Executable', srv.executable ? h('span', { class: 'mono' }, srv.executable) : undefined],
      ['Run id', srv.run_id ? h('span', { class: 'mono' }, srv.run_id) : undefined],
      ['Block cache', fmtBytes(rocks.block_cache_bytes)],
      ['Write buffers', fmtBytes(rocks.write_buffer_bytes)],
      ['Background IO', rocks.bg_io_bytes_per_sec ? `${fmtBytes(rocks.bg_io_bytes_per_sec)}/s` : undefined],
      ['Max snapshots', rocks.max_snapshots],
      ['Console', `nildb-ui${app.readonly ? ', read-only' : ''}, RESP3 to ${app.status?.nildb || ''}`],
    ]));
  }

  function schedule() {
    clearTimeout(timer);
    if (!visible) return;
    timer = setTimeout(async () => {
      if (document.visibilityState === 'visible') await refresh();
      schedule();
    }, REFRESH_MS);
  }

  return {
    el,
    show() {
      visible = true;
      refresh();
      schedule();
    },
    hide() {
      visible = false;
      clearTimeout(timer);
    },
  };
}
