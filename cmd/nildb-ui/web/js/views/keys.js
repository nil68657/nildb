// Keys: a SCAN-paged, virtualized key list with MATCH and TYPE filters,
// and a detail pane that views and edits strings, hashes, lists, sets and
// sorted sets, sets TTLs, renames and deletes.

import {
  h, clear, api, text, num, items, isErr, fmtInt, fmtBytes, fmtDuration, iconButton, button, badge,
  keyArg, keyName, keyId, escapeBytes, b64Bytes, splitCommands, copyText, debounce, ReplyError,
} from '../lib.js';
import {
  VirtualList, toast, toastError, confirmDialog, formDialog, seg, spinner, emptyState, searchInput, jsonTree, table,
} from '../ui.js';

const PAGE = 300;
const ELEM_PAGE = 200;
const STRING_LIMIT = 1 << 20;
const TYPES = ['string', 'hash', 'list', 'set', 'zset'];

const unit = { hash: ['field', 'fields'], list: ['item', 'items'], set: ['member', 'members'], zset: ['member', 'members'] };

function sizeLabel(t, n) {
  if (t === 'string') return fmtBytes(n);
  const u = unit[t];
  return u ? `${fmtInt(n)} ${n === 1 ? u[0] : u[1]}` : '';
}

// editable turns a bulk reply into text for an editor; bytes that are not
// UTF-8 are shown with \xHH escapes and parsed back on save.
function editable(v) {
  if (v && v.b64 != null) return { text: escapeBytes(b64Bytes(v.b64)), binary: true };
  return { text: (v && v.v) ?? '', binary: false };
}

function parseEscaped(s) {
  const r = splitCommands('"' + s + '"');
  if (r.error || r.cmds.length !== 1 || r.cmds[0].length !== 1) {
    throw new Error('Malformed escapes: write a quote as \\" and a byte as \\xHH');
  }
  return r.cmds[0][0];
}

const fromEditor = (s, binary) => (binary ? parseEscaped(s) : s);
const argOf = (v) => (v.b64 != null ? { b64: v.b64 } : v.v);
const shown = (v) => (v.b64 != null ? escapeBytes(b64Bytes(v.b64)) : v.v);

function hexdump(bytes, max = 64 << 10) {
  const out = [];
  const n = Math.min(bytes.length, max);
  for (let off = 0; off < n; off += 16) {
    const row = bytes.subarray(off, Math.min(off + 16, n));
    const hex = Array.from(row, (b) => b.toString(16).padStart(2, '0')).join(' ').padEnd(47, ' ');
    const asc = Array.from(row, (b) => (b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.')).join('');
    out.push(`${off.toString(16).padStart(8, '0')}  ${hex}  ${asc}`);
  }
  if (bytes.length > max) out.push(`… ${fmtInt(bytes.length - max)} more bytes`);
  return out.join('\n');
}

export function create(app) {
  const ro = () => app.readonly;
  const matchIn = searchInput({ placeholder: 'MATCH user:*', title: 'SCAN MATCH pattern: * ? [abc] and \\ work as in Redis', 'aria-label': 'Key pattern (SCAN MATCH)', class: 'input mono' });
  const matchInput = matchIn.querySelector('input');
  const typeSel = h('select', { class: 'select', 'aria-label': 'Key type (SCAN TYPE)' },
    h('option', { value: '' }, 'All types'), TYPES.map((t) => h('option', { value: t }, t)));
  const foot = h('div', { class: 'pane-foot', 'aria-live': 'polite' });
  const list = new VirtualList({
    rowHeight: 34,
    label: 'Keys',
    render: renderRow,
    onSelect: (k) => openDetail(k),
    onEnd: () => loadMore(),
  });
  list.el.addEventListener('keydown', (e) => {
    if ((e.key === 'Delete' || e.key === 'Backspace') && list.selected() && !ro()) {
      e.preventDefault();
      deleteKey(list.selected());
    }
  });
  const left = h('div', { class: 'pane' },
    h('div', { class: 'pane-head' },
      h('div', { class: 'grow' }, matchIn),
      typeSel,
      ro() ? null : button('New key', () => newKey(), { cls: 'sm primary', iconName: 'plus' })),
    list.el,
    foot);
  const detail = h('div', { class: 'pane' });
  const sub = h('span', { class: 'sub' });
  const el = h('section', { class: 'view', 'aria-label': 'Keys' },
    h('div', { class: 'view-head' }, h('h1', null, 'Keys'), sub, h('div', { class: 'grow' }),
      iconButton('refresh', 'Rescan keys', () => start())),
    h('div', { class: 'split' }, left, detail));

  // scan state
  let gen = 0;
  let cursor = '0';
  let done = false;
  let loading = false;
  let calls = 0;
  let scanMs = 0;
  let seen = new Set();
  let error = '';

  function renderRow(k) {
    return [
      badge(k.t),
      h('span', { class: 'k', title: keyName(k) }, keyName(k)),
      k.ttl > 0 ? h('span', { class: 'meta ttl', title: 'Time to live' }, fmtDuration(k.ttl)) : null,
      h('span', { class: 'meta' }, sizeLabel(k.t, k.n)),
    ];
  }

  function updateFoot() {
    const total = app.keyspace[`db${app.db}`]?.keys;
    const filtered = matchInput.value.trim() || typeSel.value;
    const parts = [];
    if (error) parts.push(h('span', { class: 'r-err' }, error));
    else if (done) parts.push(`${fmtInt(list.items.length)} ${filtered ? 'matching ' : ''}keys`);
    else parts.push(`${fmtInt(list.items.length)} loaded${total && !filtered ? ` of ${fmtInt(total)}` : ''}`);
    if (loading) parts.push(spinner('Scanning'));
    parts.push(h('span', { class: 'grow' }), `${fmtInt(calls)} SCAN ${calls === 1 ? 'call' : 'calls'} · ${Math.round(scanMs)} ms`);
    clear(foot, parts);
    sub.textContent = `db ${app.db}${filtered ? ` · MATCH ${matchInput.value.trim() || '*'}${typeSel.value ? ` TYPE ${typeSel.value}` : ''}` : ''}`;
  }

  function start() {
    gen++;
    cursor = '0';
    done = false;
    loading = false;
    calls = 0;
    scanMs = 0;
    error = '';
    seen = new Set();
    list.sel = -1;
    list.setItems([]);
    updateFoot();
    loadMore();
  }

  async function loadMore(retried = false) {
    if (loading || done) return;
    loading = true;
    const g = gen;
    updateFoot();
    try {
      const r = await api.scan({ db: app.db, cursor, match: matchInput.value.trim(), type: typeSel.value, count: PAGE });
      if (g !== gen) return;
      calls += r.calls;
      scanMs += r.ms;
      const fresh = r.keys.filter((k) => !seen.has(keyId(k)));
      for (const k of fresh) seen.add(keyId(k));
      list.append(fresh);
      cursor = r.cursor;
      done = cursor === '0';
    } catch (e) {
      if (g !== gen) return;
      if (!retried && /invalid cursor/i.test(e.message)) {
        loading = false;
        start();
        return;
      }
      error = e.message;
    } finally {
      if (g === gen) {
        loading = false;
        updateFoot();
      }
    }
    if (g === gen && !done && !error && list.items.length < Math.ceil(list.el.clientHeight / list.rowHeight) + 10) loadMore();
  }

  const restart = debounce(start, 300);
  matchInput.addEventListener('input', restart);
  matchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); start(); }
    if (e.key === 'ArrowDown') { e.preventDefault(); list.el.focus(); list.select(Math.max(0, list.sel)); }
  });
  typeSel.addEventListener('change', start);

  /* ---------------- detail ---------------- */

  let token = 0;
  let ttlTimer = 0;

  function blank() {
    token++;
    clearInterval(ttlTimer);
    clear(detail, emptyState('keys', 'Pick a key', 'Use the arrow keys or click a key to see its value. Press / to filter.'));
  }

  function updateRow(k, patch) {
    const i = list.items.findIndex((x) => keyId(x) === keyId(k));
    if (i >= 0) list.replace(i, { ...list.items[i], ...patch });
  }

  async function openDetail(k) {
    const my = ++token;
    clearInterval(ttlTimer);
    clear(detail, h('div', { class: 'empty' }, spinner(), 'Loading'));
    let t;
    let ttl;
    try {
      [t, ttl] = await api.batch([['TYPE', keyArg(k)], ['PTTL', keyArg(k)]]);
    } catch (e) {
      if (my === token) clear(detail, h('div', { class: 'empty' }, h('span', { class: 'r-err' }, e.message)));
      return;
    }
    if (my !== token) return;
    const type = text(t);
    if (type === 'none') {
      clear(detail, emptyState('keys', 'This key is gone', 'It expired or was deleted after the scan. Rescan to refresh the list.'));
      return;
    }
    const pttl = num(ttl);
    const expiresAt = pttl > 0 ? Date.now() + pttl : 0;
    const ttlEl = h('span', { class: expiresAt ? 'ttl-live' : '' });
    const sizeEl = h('span', { class: 'tnum' });
    const setTTL = () => {
      if (!expiresAt) {
        ttlEl.textContent = 'no expiry';
        return;
      }
      const left = expiresAt - Date.now();
      ttlEl.textContent = left > 0 ? `expires in ${fmtDuration(left)}` : 'expired';
      if (left <= 0) clearInterval(ttlTimer);
    };
    setTTL();
    if (expiresAt) ttlTimer = setInterval(setTTL, 1000);

    const actions = h('div', { class: 'row wrap' },
      iconButton('copy', 'Copy key name', () => copyText(keyName(k)).then(() => toast('Copied the key name', 'ok'), (e) => toastError(e))),
      iconButton('refresh', 'Reload', () => openDetail(k)),
      ro() ? null : button('Rename', () => renameKey(k), { cls: 'sm' }),
      ro() ? null : button('TTL', () => ttlKey(k, pttl), { cls: 'sm', iconName: 'clock' }),
      type === 'zset' ? button('Map', () => openInGeo(k), { cls: 'sm', title: 'Plot this GEO set in the Geo view', iconName: 'geo' }) : null,
      ro() ? null : button('Delete', () => deleteKey(k), { cls: 'sm danger', iconName: 'trash' }));
    const body = h('div', { class: 'detail-body' });
    clear(detail,
      h('div', { class: 'detail-head' },
        h('div', { class: 'detail-title' },
          h('div', { class: 'detail-name' }, keyName(k)),
          h('div', { class: 'detail-meta' }, badge(type), sizeEl, ttlEl)),
        actions),
      body);
    const ctx = {
      k, body,
      stale: () => my !== token,
      size(n) {
        sizeEl.textContent = sizeLabel(type, n);
        updateRow(k, { n, t: type });
      },
    };
    const editor = EDITORS[type];
    if (!editor) {
      body.append(h('div', { class: 'notice' }, `The console has no editor for ${type} keys; use the Console.`));
      return;
    }
    try {
      await editor(ctx);
    } catch (e) {
      if (my === token) body.append(h('div', { class: 'notice err' }, e.message));
    }
  }

  /* ---------------- editors ---------------- */

  const EDITORS = {
    async string(ctx) {
      const { k, body } = ctx;
      const [len, val] = await api.batch([['STRLEN', keyArg(k)], ['GETRANGE', keyArg(k), '0', String(STRING_LIMIT - 1)]]);
      if (ctx.stale()) return;
      const n = num(len);
      ctx.size(n);
      const cut = n > STRING_LIMIT;
      const ed = editable(val);
      let parsed = null;
      if (!ed.binary) {
        try {
          const j = JSON.parse(ed.text);
          if (j && typeof j === 'object') parsed = j;
        } catch { /* not JSON */ }
      }
      const area = h('textarea', { class: 'textarea editor', spellcheck: false, 'aria-label': 'Value', readOnly: ro() || cut }, ed.text);
      const view = h('div', { class: 'detail-body' });
      const modes = [['text', ed.binary ? 'Escaped' : 'Text']];
      if (parsed) modes.push(['json', 'JSON']);
      if (ed.binary || n <= 64 << 10) modes.push(['hex', 'Hex']);
      const saveBtn = button('Save', async () => {
        try {
          await api.ok(['SET', keyArg(k), fromEditor(area.value, ed.binary), 'KEEPTTL']);
          toast('Saved', 'ok');
          openDetail(k);
        } catch (e) {
          toastError(e, 'Save failed');
        }
      }, { cls: 'sm primary' });
      const fmtBtn = button('Format', () => {
        try { area.value = JSON.stringify(JSON.parse(area.value), null, 2); } catch (e) { toastError(e, 'Not JSON'); }
      }, { cls: 'sm' });
      const show = (m) => {
        clear(view);
        if (m === 'text') view.append(area);
        if (m === 'json') view.append(jsonTree(parsed, { depth: 2 }));
        if (m === 'hex') view.append(h('pre', { class: 'hexdump' }, hexdump(ed.binary ? b64Bytes(val.b64) : new TextEncoder().encode(ed.text))));
        saveBtn.hidden = m !== 'text' || ro() || cut;
        fmtBtn.hidden = m !== 'text' || !parsed || ro() || cut;
      };
      area.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); saveBtn.click(); }
      });
      body.append(
        h('div', { class: 'detail-tools' },
          modes.length > 1 ? seg(modes, 'text', show, 'Value view') : null,
          h('div', { class: 'grow' }),
          cut ? h('span', { class: 'pill pill-warn' }, `showing the first ${fmtBytes(STRING_LIMIT)} of ${fmtBytes(n)}, read-only`) : null,
          ed.binary ? h('span', { class: 'faint' }, 'not UTF-8: bytes shown as \\xHH') : null,
          fmtBtn, saveBtn),
        view);
      show('text');
    },

    async hash(ctx) {
      const { k, body } = ctx;
      let cursorH = '0';
      let rows = [];
      const matchBox = searchInput({ placeholder: 'Filter fields', title: 'HSCAN MATCH pattern', 'aria-label': 'Field pattern', class: 'input mono' });
      const mIn = matchBox.querySelector('input');
      const holder = h('div', { class: 'table-wrap' });
      const more = button('Load more', () => load().catch((e) => toastError(e)), { cls: 'sm' });
      body.append(
        h('div', { class: 'detail-tools' }, h('div', { class: 'grow' }, matchBox),
          ro() ? null : button('Add field', () => editField(null), { cls: 'sm primary', iconName: 'plus' })),
        holder, h('div', { class: 'detail-tools' }, more));
      const paint = () => {
        clear(holder, table(
          [{ label: 'Field' }, { label: 'Value' }, { label: '', cls: 'actions' }],
          rows.map(([f, v]) => [
            h('div', { class: 'clip mono', title: shown(f) }, shown(f)),
            h('div', { class: 'clip mono', title: shown(v).slice(0, 2000) }, shown(v).slice(0, 400)),
            ro() ? '' : h('span', { class: 'row' },
              iconButton('edit', 'Edit value', () => editField([f, v]), 'sm'),
              iconButton('trash', 'Delete field', () => delField(f), 'sm')),
          ]),
          { empty: mIn.value ? 'No field matches' : 'This hash has no fields', fixed: true }));
        more.hidden = cursorH === '0';
      };
      async function load(reset = false) {
        if (reset) { cursorH = '0'; rows = []; }
        const args = ['HSCAN', keyArg(k), cursorH, 'COUNT', String(ELEM_PAGE)];
        if (mIn.value.trim()) args.push('MATCH', mIn.value.trim());
        const [len, r] = await api.batch([['HLEN', keyArg(k)], args]);
        if (ctx.stale()) return;
        if (isErr(r)) throw new ReplyError(r.v);
        ctx.size(num(len));
        const [c, flat] = items(r);
        cursorH = text(c);
        const fv = items(flat);
        for (let i = 0; i + 1 < fv.length; i += 2) rows.push([fv[i], fv[i + 1]]);
        paint();
      }
      async function editField(pair) {
        const cur = pair ? editable(pair[1]) : { text: '', binary: false };
        const r = await formDialog({
          title: pair ? `Edit ${shown(pair[0]).slice(0, 60)}` : 'Add a field',
          fields: [
            pair ? null : { name: 'field', label: 'Field', mono: true, autofocus: true, required: true },
            { name: 'value', label: cur.binary ? 'Value (escaped bytes)' : 'Value', type: 'textarea', value: cur.text, rows: 8, autofocus: !!pair },
          ].filter(Boolean),
          submit: pair ? 'Save' : 'Add',
          onSubmit: async (v) => {
            if (!pair && !v.field) throw new Error('Name the field');
            await api.ok(['HSET', keyArg(k), pair ? argOf(pair[0]) : v.field, fromEditor(v.value, cur.binary)]);
          },
        });
        if (r) {
          toast(pair ? 'Saved' : 'Field added', 'ok');
          load(true).catch((e) => toastError(e));
        }
      }
      async function delField(f) {
        if (!await confirmDialog({ title: 'Delete this field?', name: shown(f), confirm: 'Delete field' })) return;
        try {
          await api.ok(['HDEL', keyArg(k), argOf(f)]);
          load(true);
        } catch (e) { toastError(e, 'Delete failed'); }
      }
      mIn.addEventListener('input', debounce(() => load(true).catch((e) => toastError(e)), 300));
      await load(true);
    },

    async list(ctx) {
      const { k, body } = ctx;
      let rows = [];
      let total = 0;
      const range = h('span', { class: 'faint tnum' });
      const holder = h('div', { class: 'table-wrap' });
      const more = button('Load more', () => load().catch((e) => toastError(e)), { cls: 'sm' });
      body.append(
        h('div', { class: 'detail-tools' }, range, h('div', { class: 'grow' }),
          ro() ? null : button('Push to head', () => push('LPUSH'), { cls: 'sm', iconName: 'plus' }),
          ro() ? null : button('Push to tail', () => push('RPUSH'), { cls: 'sm primary', iconName: 'plus' })),
        holder, h('div', { class: 'detail-tools' }, more));
      const paint = () => {
        clear(holder, table(
          [{ label: 'Index', num: true }, { label: 'Value' }, { label: '', cls: 'actions' }],
          rows.map((v, i) => [
            String(i),
            h('div', { class: 'clip mono', title: shown(v).slice(0, 2000) }, shown(v).slice(0, 400)),
            ro() ? '' : h('span', { class: 'row' },
              iconButton('edit', `Edit item ${i}`, () => editItem(i, v), 'sm'),
              iconButton('trash', `Remove item ${i}`, () => removeItem(i, v), 'sm')),
          ]),
          { empty: 'This list is empty', fixed: true }));
        more.hidden = rows.length >= total;
        range.textContent = total ? `items 0 to ${fmtInt(rows.length - 1)} of ${fmtInt(total)}` : '';
      };
      async function load(reset = false) {
        if (reset) rows = [];
        const [len, page] = await api.batch([['LLEN', keyArg(k)], ['LRANGE', keyArg(k), String(rows.length), String(rows.length + ELEM_PAGE - 1)]]);
        if (ctx.stale()) return;
        if (isErr(page)) throw new ReplyError(page.v);
        total = num(len);
        ctx.size(total);
        rows.push(...items(page));
        paint();
      }
      async function push(cmd) {
        const r = await formDialog({
          title: cmd === 'LPUSH' ? 'Push to the head' : 'Push to the tail',
          fields: [{ name: 'value', label: 'Value', type: 'textarea', rows: 5, autofocus: true }],
          submit: 'Push',
          onSubmit: (v) => api.ok([cmd, keyArg(k), v.value]),
        });
        if (r) load(true).catch((e) => toastError(e));
      }
      async function editItem(i, v) {
        const cur = editable(v);
        const r = await formDialog({
          title: `Edit item ${i}`,
          fields: [{ name: 'value', label: cur.binary ? 'Value (escaped bytes)' : 'Value', type: 'textarea', value: cur.text, rows: 8, autofocus: true }],
          onSubmit: (x) => api.ok(['LSET', keyArg(k), String(i), fromEditor(x.value, cur.binary)]),
        });
        if (r) {
          toast('Saved', 'ok');
          load(true).catch((e) => toastError(e));
        }
      }
      async function removeItem(i, v) {
        const ok = await confirmDialog({
          title: `Remove item ${i}?`,
          message: 'Redis has no remove-by-index, so the console sets the item to a marker and removes the marker, in one MULTI.',
          name: shown(v).slice(0, 300), confirm: 'Remove',
        });
        if (!ok) return;
        const marker = `__nildb_ui_removed_${crypto.randomUUID()}`;
        try {
          await api.atomic([['LSET', keyArg(k), String(i), marker], ['LREM', keyArg(k), '1', marker]]);
          load(true);
        } catch (e) { toastError(e, 'Remove failed'); }
      }
      await load(true);
    },

    async set(ctx) {
      const { k, body } = ctx;
      let cursorS = '0';
      let rows = [];
      const matchBox = searchInput({ placeholder: 'Filter members', title: 'SSCAN MATCH pattern', 'aria-label': 'Member pattern', class: 'input mono' });
      const mIn = matchBox.querySelector('input');
      const holder = h('div', { class: 'table-wrap' });
      const more = button('Load more', () => load().catch((e) => toastError(e)), { cls: 'sm' });
      body.append(
        h('div', { class: 'detail-tools' }, h('div', { class: 'grow' }, matchBox),
          ro() ? null : button('Add member', () => add(), { cls: 'sm primary', iconName: 'plus' })),
        holder, h('div', { class: 'detail-tools' }, more));
      const paint = () => {
        clear(holder, table(
          [{ label: 'Member' }, { label: '', cls: 'actions' }],
          rows.map((m) => [
            h('div', { class: 'clip mono', title: shown(m).slice(0, 2000) }, shown(m).slice(0, 400)),
            ro() ? '' : iconButton('trash', 'Remove member', () => remove(m), 'sm'),
          ]),
          { empty: mIn.value ? 'No member matches' : 'This set is empty', fixed: true }));
        more.hidden = cursorS === '0';
      };
      async function load(reset = false) {
        if (reset) { cursorS = '0'; rows = []; }
        const args = ['SSCAN', keyArg(k), cursorS, 'COUNT', String(ELEM_PAGE)];
        if (mIn.value.trim()) args.push('MATCH', mIn.value.trim());
        const [card, r] = await api.batch([['SCARD', keyArg(k)], args]);
        if (ctx.stale()) return;
        if (isErr(r)) throw new ReplyError(r.v);
        ctx.size(num(card));
        const [c, ms] = items(r);
        cursorS = text(c);
        rows.push(...items(ms));
        paint();
      }
      async function add() {
        const r = await formDialog({
          title: 'Add a member',
          fields: [{ name: 'member', label: 'Member', type: 'textarea', rows: 3, autofocus: true }],
          submit: 'Add',
          onSubmit: async (v) => {
            const res = await api.ok(['SADD', keyArg(k), v.member]);
            if (num(res) === 0) throw new Error('That member is already in the set');
          },
        });
        if (r) load(true).catch((e) => toastError(e));
      }
      async function remove(m) {
        if (!await confirmDialog({ title: 'Remove this member?', name: shown(m).slice(0, 300), confirm: 'Remove' })) return;
        try {
          await api.ok(['SREM', keyArg(k), argOf(m)]);
          load(true);
        } catch (e) { toastError(e, 'Remove failed'); }
      }
      mIn.addEventListener('input', debounce(() => load(true).catch((e) => toastError(e)), 300));
      await load(true);
    },

    async zset(ctx) {
      const { k, body } = ctx;
      let rows = [];
      let total = 0;
      let rev = false;
      let cursorZ = '0';
      const matchBox = searchInput({ placeholder: 'Filter members', title: 'ZSCAN MATCH pattern', 'aria-label': 'Member pattern', class: 'input mono' });
      const mIn = matchBox.querySelector('input');
      const holder = h('div', { class: 'table-wrap' });
      const more = button('Load more', () => load().catch((e) => toastError(e)), { cls: 'sm' });
      const order = seg([['asc', 'Lowest first'], ['desc', 'Highest first']], 'asc', (v) => {
        rev = v === 'desc';
        load(true).catch((e) => toastError(e));
      }, 'Order');
      body.append(
        h('div', { class: 'detail-tools' }, h('div', { class: 'grow' }, matchBox), order,
          ro() ? null : button('Add member', () => edit(null), { cls: 'sm primary', iconName: 'plus' })),
        holder, h('div', { class: 'detail-tools' }, more));
      const filtering = () => mIn.value.trim() !== '';
      const paint = () => {
        clear(holder, table(
          [{ label: filtering() ? '' : 'Rank', num: true }, { label: 'Member' }, { label: 'Score', num: true }, { label: '', cls: 'actions' }],
          rows.map(([m, s], i) => [
            filtering() ? '' : String(rev ? total - 1 - i : i),
            h('div', { class: 'clip mono', title: shown(m).slice(0, 2000) }, shown(m).slice(0, 400)),
            h('span', { class: 'mono' }, text(s)),
            ro() ? '' : h('span', { class: 'row' },
              iconButton('edit', 'Change score', () => edit([m, s]), 'sm'),
              iconButton('trash', 'Remove member', () => remove(m), 'sm')),
          ]),
          { empty: filtering() ? 'No member matches' : 'This sorted set is empty', fixed: true }));
        more.hidden = filtering() ? cursorZ === '0' : rows.length >= total;
      };
      async function load(reset = false) {
        if (reset) { rows = []; cursorZ = '0'; }
        if (filtering()) {
          const [card, r] = await api.batch([['ZCARD', keyArg(k)], ['ZSCAN', keyArg(k), cursorZ, 'COUNT', String(ELEM_PAGE), 'MATCH', mIn.value.trim()]]);
          if (ctx.stale()) return;
          if (isErr(r)) throw new ReplyError(r.v);
          total = num(card);
          const [c, flat] = items(r);
          cursorZ = text(c);
          const ms = items(flat);
          for (let i = 0; i + 1 < ms.length; i += 2) rows.push([ms[i], ms[i + 1]]);
        } else {
          const args = ['ZRANGE', keyArg(k), String(rows.length), String(rows.length + ELEM_PAGE - 1)];
          if (rev) args.push('REV');
          args.push('WITHSCORES');
          const [card, r] = await api.batch([['ZCARD', keyArg(k)], args]);
          if (ctx.stale()) return;
          if (isErr(r)) throw new ReplyError(r.v);
          total = num(card);
          for (const pair of items(r)) {
            const [m, s] = items(pair);
            rows.push([m, s]);
          }
        }
        ctx.size(total);
        paint();
      }
      async function edit(pair) {
        const r = await formDialog({
          title: pair ? `Score of ${shown(pair[0]).slice(0, 60)}` : 'Add a member',
          fields: [
            pair ? null : { name: 'member', label: 'Member', mono: true, autofocus: true },
            { name: 'score', label: 'Score', mono: true, value: pair ? text(pair[1]) : '', autofocus: !!pair, hint: 'A float; inf and -inf work too' },
          ].filter(Boolean),
          submit: pair ? 'Save' : 'Add',
          onSubmit: (v) => api.ok(pair ? ['ZADD', keyArg(k), 'XX', v.score, argOf(pair[0])] : ['ZADD', keyArg(k), v.score, v.member]),
        });
        if (r) load(true).catch((e) => toastError(e));
      }
      async function remove(m) {
        if (!await confirmDialog({ title: 'Remove this member?', name: shown(m).slice(0, 300), confirm: 'Remove' })) return;
        try {
          await api.ok(['ZREM', keyArg(k), argOf(m)]);
          load(true);
        } catch (e) { toastError(e, 'Remove failed'); }
      }
      mIn.addEventListener('input', debounce(() => load(true).catch((e) => toastError(e)), 300));
      await load(true);
    },
  };

  /* ---------------- key actions ---------------- */

  async function deleteKey(k) {
    const ok = await confirmDialog({
      title: 'Delete this key?',
      message: `DEL removes it from database ${app.db} with every element it holds. There is no undo.`,
      name: keyName(k),
      confirm: 'Delete key',
    });
    if (!ok) return;
    try {
      await api.ok(['DEL', keyArg(k)]);
      const i = list.items.findIndex((x) => keyId(x) === keyId(k));
      if (i >= 0) {
        list.removeAt(i);
        seen.delete(keyId(k));
      }
      toast(`Deleted ${keyName(k)}`, 'ok');
      if (list.selected()) list.select(list.sel);
      else blank();
      updateFoot();
    } catch (e) {
      toastError(e, 'Delete failed');
    }
  }

  async function renameKey(k) {
    const res = await formDialog({
      title: 'Rename key',
      fields: [{ name: 'name', label: 'New name', mono: true, value: k.b64 ? '' : k.k, autofocus: true, required: true }],
      submit: 'Rename',
      onSubmit: async (v) => {
        if (!v.name) throw new Error('Type the new name');
        const r = await api.ok(['RENAMENX', keyArg(k), v.name]);
        if (num(r) === 1) return { name: v.name };
        const over = await confirmDialog({ title: 'Replace the existing key?', message: 'A key with that name exists. RENAME overwrites it.', name: v.name, confirm: 'Replace' });
        if (!over) throw new Error('Kept both keys; pick another name');
        await api.ok(['RENAME', keyArg(k), v.name]);
        return { name: v.name };
      },
    });
    if (!res) return;
    const nk = { k: res.name };
    const i = list.items.findIndex((x) => keyId(x) === keyId(k));
    if (i >= 0) {
      seen.delete(keyId(k));
      seen.add(keyId(nk));
      list.replace(i, { ...list.items[i], k: res.name, b64: undefined });
    }
    toast('Renamed', 'ok');
    openDetail(i >= 0 ? list.items[i] : nk);
  }

  async function ttlKey(k, pttl) {
    const res = await formDialog({
      title: 'Time to live',
      intro: 'Seconds until NilDB deletes the key. Leave empty to remove the expiry.',
      fields: [{ name: 'secs', label: 'Seconds', type: 'number', min: 1, step: 1, value: pttl > 0 ? String(Math.ceil(pttl / 1000)) : '', autofocus: true }],
      submit: 'Apply',
      onSubmit: async (v) => {
        const s = v.secs.trim();
        if (!s) {
          await api.ok(['PERSIST', keyArg(k)]);
          return { ttl: -1 };
        }
        if (!/^\d+$/.test(s) || Number(s) < 1) throw new Error('Seconds must be a whole number above 0');
        await api.ok(['EXPIRE', keyArg(k), s]);
        return { ttl: Number(s) * 1000 };
      },
    });
    if (!res) return;
    updateRow(k, { ttl: res.ttl });
    openDetail(k);
  }

  function openInGeo(k) {
    app.go('geo');
    app.view('geo').then((g) => g.openSet(k));
  }

  const lines = (txt) => txt.split('\n').map((l) => l.replace(/\r$/, '')).filter((l) => l.length);

  async function newKey() {
    if (ro()) return;
    const res = await formDialog({
      title: `New key in db ${app.db}`,
      wide: true,
      fields: [
        { name: 'name', label: 'Name', mono: true, autofocus: true, required: true, placeholder: 'user:1001' },
        { name: 'type', label: 'Type', type: 'select', value: 'string', options: TYPES.map((t) => [t, t]) },
        {
          name: 'value', label: 'Value', type: 'textarea', rows: 6,
          hint: 'string: the value. hash: "field value" per line. list and set: one element per line. zset: "score member" per line.',
        },
        { name: 'ttl', label: 'TTL in seconds (optional)', type: 'number', min: 1, step: 1 },
      ],
      submit: 'Create',
      onSubmit: async (v) => {
        if (!v.name) throw new Error('Name the key');
        if (v.ttl && !/^\d+$/.test(v.ttl)) throw new Error('TTL must be a whole number of seconds');
        if (num(await api.run(['EXISTS', v.name])) === 1) throw new Error('A key with this name already exists');
        const ls = lines(v.value);
        const pair = (l) => {
          const i = l.indexOf(' ');
          if (i <= 0) throw new Error(`"${l}" needs two parts separated by a space`);
          return [l.slice(0, i), l.slice(i + 1)];
        };
        if (v.type !== 'string' && !ls.length) throw new Error(`A ${v.type} needs at least one element`);
        const cmd = {
          string: () => ['SET', v.name, v.value],
          hash: () => ['HSET', v.name, ...ls.flatMap(pair)],
          list: () => ['RPUSH', v.name, ...ls],
          set: () => ['SADD', v.name, ...ls],
          zset: () => ['ZADD', v.name, ...ls.flatMap(pair)],
        }[v.type]();
        const cmds = [cmd];
        if (v.ttl) cmds.push(['EXPIRE', v.name, v.ttl]);
        await api.atomic(cmds);
        return { k: v.name };
      },
    });
    if (!res) return;
    toast(`Created ${res.k}`, 'ok');
    start();
    openDetail({ k: res.k });
  }

  function openKey(k) {
    const i = list.items.findIndex((x) => keyId(x) === keyId(k));
    if (i >= 0) list.select(i);
    else openDetail(k);
  }

  app.on('db', () => {
    start();
    blank();
  });

  let started = false;
  blank();
  return {
    el,
    show() {
      if (!started) {
        started = true;
        start();
      }
    },
    focusSearch() {
      matchInput.focus();
      matchInput.select();
    },
    newKey,
    openKey,
  };
}
