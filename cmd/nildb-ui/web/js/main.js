// The console shell: navigation, the Cmd-K palette, keyboard shortcuts,
// theme, database picker and the connection pulse.

import {
  h, clear, icon, api, text, items, parseInfo, parseKV, fmtInt, fmtMs, debounce,
  keyName, store, badge,
} from './lib.js';
import { toast, infoDialog, errorText } from './ui.js';

const VIEWS = [
  { id: 'overview', title: 'Overview', icon: 'overview', key: 'o', load: () => import('./views/overview.js') },
  { id: 'keys', title: 'Keys', icon: 'keys', key: 'k', load: () => import('./views/keys.js') },
  { id: 'console', title: 'Console', icon: 'console', key: 'c', load: () => import('./views/console.js') },
  { id: 'docs', title: 'Documents', icon: 'docs', key: 'd', load: () => import('./views/documents.js') },
  { id: 'geo', title: 'Geo', icon: 'geo', key: 'm', load: () => import('./views/geo.js') },
  { id: 'analytics', title: 'Analytics', icon: 'analytics', key: 'a', load: () => import('./views/analytics.js') },
];

const PULSE_MS = 2000;
const PULSE_KEEP = 150;

// app is the state the views share.
const app = {
  db: 0,
  readonly: false,
  status: null,
  commands: [],
  commandMap: new Map(),
  keyspace: {},
  server: {},
  pulse: [],
  views: {},
  current: null,
  session: crypto.randomUUID(),
  listeners: {},
  on(ev, fn) { (this.listeners[ev] ||= []).push(fn); },
  emit(ev, ...args) { for (const fn of this.listeners[ev] || []) fn(...args); },
  go(path) {
    const next = '#/' + path;
    if (location.hash === next) route();
    else location.hash = next;
  },
  setDB(n) {
    n = Number(n);
    if (!Number.isInteger(n) || n < 0 || n > 15 || n === this.db) return;
    this.db = n;
    api.db = n;
    store.set('db', n);
    dbSelect.value = String(n);
    this.emit('db', n);
  },
  // view returns a view, loading it once when first asked for.
  loading: {},
  view(id) {
    this.loading[id] ||= VIEWS.find((v) => v.id === id).load().then((mod) => {
      const v = mod.create(this);
      v.el.hidden = true;
      main.append(v.el);
      this.views[id] = v;
      return v;
    });
    return this.loading[id];
  },
  command(name) { return this.commandMap.get(String(name).toLowerCase()); },
  blocked(name) { return !!this.command(name)?.blocked; },
};

const main = document.getElementById('main');
const nav = document.getElementById('nav');
const dbSelect = document.getElementById('db-select');
const connEl = document.getElementById('conn');
const themeBtn = document.getElementById('theme-btn');
const isMac = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);

/* ---------------- navigation ---------------- */

for (const v of VIEWS) {
  nav.append(h('a', { class: 'nav-link', href: `#/${v.id}`, dataset: { view: v.id }, 'aria-keyshortcuts': `g ${v.key}` },
    icon(v.icon), h('span', null, v.title), h('kbd', null, `G ${v.key.toUpperCase()}`)));
}
nav.append(h('div', { class: 'sidebar-foot', id: 'nav-foot' }));

async function route() {
  const m = location.hash.match(/^#\/([a-z]+)(?:\/(.*))?$/);
  const id = m && VIEWS.some((v) => v.id === m[1]) ? m[1] : 'overview';
  const param = m && m[2] ? decodeURIComponent(m[2]) : '';
  for (const a of nav.querySelectorAll('.nav-link')) {
    if (a.dataset.view === id) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  }
  if (app.current && app.current !== id) {
    const prev = app.views[app.current];
    prev.el.hidden = true;
    prev.hide?.();
  }
  let v;
  try {
    v = await app.view(id);
  } catch (e) {
    toast('Could not load the view: ' + errorText(e), 'err');
    return;
  }
  const changed = app.current !== id;
  app.current = id;
  v.el.hidden = false;
  v.show?.(param, changed);
  document.title = `${VIEWS.find((x) => x.id === id).title} · NilDB console`;
}

window.addEventListener('hashchange', route);

/* ---------------- theme ---------------- */

function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  try { localStorage.setItem('nildb-ui.theme', t); } catch { /* ignore */ }
  const next = t === 'dark' ? 'light' : 'dark';
  themeBtn.setAttribute('aria-label', `Switch to ${next} theme`);
  themeBtn.title = `Switch to ${next} theme`;
  clear(themeBtn, icon(t === 'dark' ? 'sun' : 'moon'));
  app.emit('theme', t);
}
const toggleTheme = () => applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');
themeBtn.addEventListener('click', toggleTheme);
applyTheme(document.documentElement.dataset.theme === 'light' ? 'light' : 'dark');

/* ---------------- database picker ---------------- */

function fillDBs() {
  const cur = String(app.db);
  clear(dbSelect, Array.from({ length: 16 }, (_, i) => {
    const ks = app.keyspace[`db${i}`];
    return h('option', { value: String(i) }, ks ? `${i}  (${fmtInt(ks.keys)})` : String(i));
  }));
  dbSelect.value = cur;
}

function setKeyspace(section) {
  app.keyspace = {};
  for (const [db, v] of Object.entries(section || {})) app.keyspace[db] = parseKV(v);
  fillDBs();
}

async function refreshKeyspace() {
  try {
    setKeyspace(parseInfo(text(await api.run(['INFO', 'keyspace']))).keyspace);
  } catch { /* the pulse reports connection problems */ }
}

dbSelect.addEventListener('change', () => app.setDB(dbSelect.value));
dbSelect.addEventListener('focus', refreshKeyspace);
app.on('info', (info) => setKeyspace(info.keyspace));

/* ---------------- connection pulse ---------------- */

function setConn(ok, msg, title = '') {
  connEl.classList.toggle('ok', ok);
  connEl.classList.toggle('err', !ok);
  connEl.querySelector('.conn-text').textContent = msg;
  connEl.title = title;
}

let pulseTimer = 0;
let lastOK = false;

async function pulse() {
  clearTimeout(pulseTimer);
  if (document.visibilityState === 'hidden') {
    pulseTimer = setTimeout(pulse, PULSE_MS);
    return;
  }
  const t0 = performance.now();
  try {
    const info = parseInfo(text(await api.run(['INFO', 'stats'], { db: 0 })));
    const st = info.stats || {};
    app.pulse.push({ t: Date.now(), ops: Number(st.instantaneous_ops_per_sec) || 0, cmds: Number(st.total_commands_processed) || 0 });
    if (app.pulse.length > PULSE_KEEP) app.pulse.shift();
    const ms = performance.now() - t0;
    const engine = app.server.nildb_engine ? ` · ${app.server.nildb_engine}` : '';
    setConn(true, `${app.status?.nildb || 'NilDB'}${engine} · ${fmtMs(ms)}`, `NilDB at ${app.status?.nildb}, round trip ${fmtMs(ms)}`);
    if (!lastOK) {
      lastOK = true;
      await loadServer();
    }
    app.emit('pulse', app.pulse);
  } catch (e) {
    lastOK = false;
    setConn(false, `No connection: ${errorText(e)}`, errorText(e));
    app.emit('down', e);
  }
  pulseTimer = setTimeout(pulse, PULSE_MS);
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') pulse();
});

// loadServer reads what changes only when NilDB restarts: the server
// section of INFO and the command table.
async function loadServer() {
  try {
    const [srv, cmds] = await Promise.all([api.run(['INFO', 'server'], { db: 0 }), api.commands()]);
    app.server = parseInfo(text(srv)).server || {};
    app.commands = cmds.commands;
    app.commandMap = new Map();
    for (const c of cmds.commands) {
      app.commandMap.set(c.name, c);
      for (const s of c.subcommands || []) app.commandMap.set(s.name, s);
    }
    const ver = app.server.nildb_engine_version || app.server.nildb_rocksdb_version || '';
    clear(document.getElementById('nav-foot'),
      h('div', null, `Redis ${app.server.redis_version || '?'} protocol`),
      h('div', null, `${app.server.nildb_engine || 'rocksdb'} ${ver}`));
    app.emit('server', app.server);
    refreshKeyspace();
  } catch (e) {
    toast('Could not read the command table: ' + errorText(e), 'err');
  }
}

async function start() {
  app.db = Math.min(15, Math.max(0, Number(store.get('db', 0)) || 0));
  api.db = app.db;
  fillDBs();
  document.getElementById('palette-kbd').textContent = isMac ? '⌘K' : 'Ctrl K';
  try {
    app.status = await api.status();
    app.readonly = app.status.readonly;
    document.getElementById('ro-badge').hidden = !app.readonly;
    document.documentElement.classList.toggle('readonly', app.readonly);
  } catch (e) {
    setConn(false, errorText(e));
  }
  window.addEventListener('pagehide', () => api.closeSession(app.session));
  route();
  pulse();
}

/* ---------------- keyboard ---------------- */

const isTyping = (el) => !!el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName));
let gArmed = 0;

document.addEventListener('keydown', (e) => {
  const mod = e.metaKey || e.ctrlKey;
  if (mod && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    openPalette();
    return;
  }
  if (e.defaultPrevented || mod || e.altKey || isTyping(e.target) || document.querySelector('dialog[open]')) return;
  if (gArmed) {
    clearTimeout(gArmed);
    gArmed = 0;
    const v = VIEWS.find((x) => x.key === e.key.toLowerCase());
    if (v) {
      e.preventDefault();
      app.go(v.id);
    }
    return;
  }
  switch (e.key) {
    case 'g':
      gArmed = setTimeout(() => { gArmed = 0; }, 1200);
      e.preventDefault();
      break;
    case '/':
      if (app.views[app.current]?.focusSearch) {
        e.preventDefault();
        app.views[app.current].focusSearch();
      }
      break;
    case '?':
      e.preventDefault();
      showShortcuts();
      break;
  }
});

function showShortcuts() {
  const mk = isMac ? '⌘' : 'Ctrl';
  const rows = [
    [`${mk} K`, 'Search keys, collections and commands, or run an action'],
    ...VIEWS.map((v) => [`G ${v.key.toUpperCase()}`, `Go to ${v.title}`]),
    ['/', 'Focus the search or filter of the current view'],
    ['?', 'Show this list'],
    ['↑ ↓ Enter', 'Move through the key list and open a key'],
    ['Delete', 'Delete the selected key (asks first)'],
    ['↑ ↓ in Console', 'Walk the command history'],
    ['Tab in Console', 'Complete a command name'],
    [`${mk} L in Console`, 'Clear the output'],
    [`${mk} Enter`, 'Run the query in Documents and Analytics'],
  ];
  infoDialog({
    title: 'Keyboard shortcuts',
    body: h('table', { class: 'table' }, h('tbody', null, rows.map(([k, d]) => h('tr', null, h('td', null, h('kbd', null, k)), h('td', null, d))))),
  });
}

/* ---------------- palette ---------------- */

// fuzzy scores txt against q as an ordered subsequence and returns the
// matched positions, or null.
function fuzzy(q, txt) {
  if (!q) return { score: 0, at: [] };
  const t = txt.toLowerCase();
  const at = [];
  let from = 0;
  let score = 0;
  let prev = -2;
  for (const ch of q.toLowerCase()) {
    const j = t.indexOf(ch, from);
    if (j < 0) return null;
    score += j === prev + 1 ? 4 : 1;
    if (j === 0 || /[\s._:|/-]/.test(t[j - 1])) score += 3;
    at.push(j);
    prev = j;
    from = j + 1;
  }
  if (t.startsWith(q.toLowerCase())) score += 8;
  return { score: score - t.length * 0.02, at };
}

function marked(txt, at) {
  if (!at.length) return txt;
  const set = new Set(at);
  const out = [];
  let buf = '';
  let inMark = false;
  for (let i = 0; i < txt.length; i++) {
    const m = set.has(i);
    if (m !== inMark) {
      if (buf) out.push(inMark ? h('mark', null, buf) : buf);
      buf = '';
      inMark = m;
    }
    buf += txt[i];
  }
  if (buf) out.push(inMark ? h('mark', null, buf) : buf);
  return out;
}

const globEscape = (s) => s.replace(/[*?[\]\\]/g, '\\$&');

let collCache = { at: 0, list: [] };
async function collections() {
  if (Date.now() - collCache.at < 10000) return collCache.list;
  const r = await api.run(['DOC.COLLECTIONS']);
  collCache = { at: Date.now(), list: items(r).map(text) };
  return collCache.list;
}

function staticItems() {
  const out = VIEWS.map((v) => ({
    group: 'Views', title: `Go to ${v.title}`, sub: `G ${v.key.toUpperCase()}`, icon: v.icon, run: () => app.go(v.id),
  }));
  out.push(
    { group: 'Actions', title: 'Toggle light and dark theme', icon: 'theme', run: toggleTheme },
    { group: 'Actions', title: 'Keyboard shortcuts', sub: '?', icon: 'keyboard', run: showShortcuts },
    { group: 'Actions', title: 'New key', icon: 'plus', run: async () => { app.go('keys'); (await app.view('keys')).newKey?.(); } },
    { group: 'Actions', title: 'New collection', icon: 'plus', run: async () => { app.go('docs'); (await app.view('docs')).newCollection?.(); } },
    { group: 'Actions', title: 'Create a snapshot lease', icon: 'lease', run: async () => { app.go('analytics'); (await app.view('analytics')).createLease?.(); } },
    { group: 'Actions', title: 'Clear Console output', icon: 'console', run: async () => { (await app.view('console')).clear?.(); } },
  );
  for (let i = 0; i < 16; i++) {
    const ks = app.keyspace[`db${i}`];
    out.push({ group: 'Databases', title: `Use database ${i}`, sub: ks ? `${fmtInt(ks.keys)} keys` : 'empty', icon: 'db', hidden: true, run: () => app.setDB(i) });
  }
  return out;
}

function openPalette() {
  if (document.querySelector('dialog.palette[open]')) return;
  const input = h('input', {
    class: 'palette-input', type: 'text', placeholder: 'Search keys, collections, commands and actions',
    'aria-label': 'Search', autocomplete: 'off', spellcheck: false, role: 'combobox', 'aria-expanded': 'true',
    'aria-controls': 'palette-list',
  });
  const list = h('div', { class: 'palette-list', role: 'listbox', id: 'palette-list', 'aria-label': 'Results' });
  const dlg = h('dialog', { class: 'palette', 'aria-label': 'Command palette' },
    h('div', { class: 'palette-top' }, icon('search'), input),
    list,
    h('div', { class: 'palette-foot' },
      h('span', null, h('kbd', null, '↑↓'), ' move'), h('span', null, h('kbd', null, 'Enter'), ' open'), h('span', null, h('kbd', null, 'Esc'), ' close')));
  document.body.append(dlg);
  dlg.addEventListener('close', () => dlg.remove());

  let results = [];
  let active = 0;
  let dynamic = [];
  let seq = 0;
  const base = staticItems();
  const order = ['Keys', 'Collections', 'Views', 'Actions', 'Databases', 'Commands'];

  function render() {
    const q = input.value.trim();
    results = [];
    for (const it of [...base, ...dynamic]) {
      if (!q && it.hidden) continue;
      if (it.always) {
        results.push({ it, m: { score: 100, at: [] } });
        continue;
      }
      const m = fuzzy(q, it.title);
      if (m) results.push({ it, m });
    }
    if (q) {
      results.sort((a, b) => b.m.score - a.m.score);
      results = results.slice(0, 50);
      results.sort((a, b) => order.indexOf(a.it.group) - order.indexOf(b.it.group));
    }
    active = Math.min(active, Math.max(0, results.length - 1));
    list.replaceChildren();
    if (!results.length) {
      list.append(h('div', { class: 'palette-empty' }, q ? `Nothing matches “${q}”` : 'Type to search'));
      input.removeAttribute('aria-activedescendant');
      return;
    }
    let group = '';
    results.forEach(({ it, m }, i) => {
      if (it.group !== group) {
        group = it.group;
        list.append(h('div', { class: 'palette-group', role: 'presentation' }, group));
      }
      list.append(h('div', {
        class: 'palette-item', role: 'option', id: `pal-${i}`, 'aria-selected': String(i === active),
        onClick: () => pick(i), onMousemove: () => { if (active !== i) { active = i; mark(); } },
      }, it.badge || icon(it.icon || 'arrow'), h('span', { class: 't' }, marked(it.title, m.at)), it.sub ? h('span', { class: 's' }, it.sub) : null));
    });
    mark();
  }

  function mark() {
    list.querySelectorAll('.palette-item').forEach((el, i) => el.setAttribute('aria-selected', String(i === active)));
    const el = list.querySelector(`#pal-${active}`);
    if (el) {
      input.setAttribute('aria-activedescendant', el.id);
      el.scrollIntoView({ block: 'nearest' });
    }
  }

  function pick(i) {
    const r = results[i];
    if (!r) return;
    dlg.close();
    Promise.resolve(r.it.run()).catch((e) => toast(errorText(e), 'err'));
  }

  const lookup = debounce(async () => {
    const q = input.value.trim();
    const mine = ++seq;
    if (!q) {
      dynamic = [];
      render();
      return;
    }
    const found = [];
    const tasks = [
      api.scan({ db: app.db, match: `*${globEscape(q)}*`, count: 30 }).then((r) => {
        for (const k of r.keys.slice(0, 8)) {
          found.push({
            group: 'Keys', title: keyName(k), sub: `db ${app.db}`, badge: badge(k.t), always: true,
            run: async () => { app.go('keys'); (await app.view('keys')).openKey(k); },
          });
        }
      }).catch(() => {}),
      collections().then((cs) => {
        for (const ns of cs) {
          found.push({ group: 'Collections', title: ns, sub: 'collection', icon: 'docs', run: () => app.go(`docs/${encodeURIComponent(ns)}`) });
        }
      }).catch(() => {}),
    ];
    for (const c of app.commands) {
      found.push({
        group: 'Commands', title: c.name.toUpperCase(), sub: c.summary || '', icon: 'console',
        run: async () => { app.go('console'); (await app.view('console')).prefill(c.name.toUpperCase() + ' '); },
      });
    }
    await Promise.all(tasks);
    if (mine !== seq) return;
    dynamic = found;
    render();
  }, 140);

  input.addEventListener('input', () => {
    active = 0;
    render();
    lookup();
  });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') { active = Math.min(results.length - 1, active + 1); mark(); e.preventDefault(); }
    else if (e.key === 'ArrowUp') { active = Math.max(0, active - 1); mark(); e.preventDefault(); }
    else if (e.key === 'Enter') { pick(active); e.preventDefault(); }
  });
  dlg.showModal();
  input.focus();
  render();
}

document.getElementById('palette-btn').addEventListener('click', openPalette);

start();
