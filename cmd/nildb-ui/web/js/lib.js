// Shared helpers: DOM building, formatting, the console API, reply values,
// redis-cli argument splitting and quoting, INFO parsing, and MongoDB
// Extended JSON display.

/* ---------------- DOM ---------------- */

const attrOnly = new Set(['role', 'for', 'form', 'list', 'viewBox']);

export function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  if (props) setProps(el, props);
  append(el, kids);
  return el;
}

export function setProps(el, props) {
  for (const [k, v] of Object.entries(props)) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'style') Object.assign(el.style, v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2).toLowerCase(), v);
    else if (k.includes('-') || attrOnly.has(k) || !(k in el)) el.setAttribute(k, v === true ? '' : String(v));
    else el[k] = v;
  }
}

export function append(el, kids) {
  for (const kid of kids) {
    if (kid == null || kid === false) continue;
    if (Array.isArray(kid)) append(el, kid);
    else el.append(kid instanceof Node ? kid : String(kid));
  }
  return el;
}

export function clear(el, ...kids) {
  el.replaceChildren();
  return append(el, kids);
}

const tpl = document.createElement('template');
export function svgEl(markup) {
  tpl.innerHTML = markup.trim();
  return tpl.content.firstElementChild;
}

const ICONS = {
  overview: '<rect x="3.5" y="3.5" width="7" height="7" rx="1.6"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.6"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.6"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.6"/>',
  keys: '<circle cx="8" cy="15.5" r="4.2"/><path d="m11 12.5 8.5-8.5M15.8 7.7l2.5 2.5M13.4 10.1l2 2"/>',
  console: '<rect x="3" y="4.5" width="18" height="15" rx="2.5"/><path d="m7.5 9.5 3 2.5-3 2.5M12.5 15h4"/>',
  docs: '<path d="M8.5 3.5H7a2 2 0 0 0-2 2v4.2L3.5 12 5 14.3v4.2a2 2 0 0 0 2 2h1.5M15.5 3.5H17a2 2 0 0 1 2 2v4.2l1.5 2.3-1.5 2.3v4.2a2 2 0 0 1-2 2h-1.5"/>',
  geo: '<circle cx="12" cy="12" r="8.5"/><path d="M3.5 12h17M12 3.5c2.4 2.3 3.6 5.1 3.6 8.5s-1.2 6.2-3.6 8.5c-2.4-2.3-3.6-5.1-3.6-8.5S9.6 5.8 12 3.5z"/>',
  analytics: '<path d="M3.5 20.5h17"/><rect x="5" y="11" width="3" height="7" rx="1"/><rect x="10.5" y="5" width="3" height="13" rx="1"/><rect x="16" y="8.5" width="3" height="9.5" rx="1"/>',
  search: '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  trash: '<path d="M4.5 7h15M9.5 7V4.5h5V7M6.5 7l.8 12.1a1.5 1.5 0 0 0 1.5 1.4h6.4a1.5 1.5 0 0 0 1.5-1.4L17.5 7"/>',
  refresh: '<path d="M19.5 12a7.5 7.5 0 1 1-2.2-5.3M19.5 4.5v4h-4"/>',
  edit: '<path d="M4.5 19.5h4l10-10a2.1 2.1 0 0 0-4-4l-10 10v4zM13.5 6.5l4 4"/>',
  copy: '<rect x="8.5" y="8.5" width="11" height="11" rx="2"/><path d="M15.5 8.5V6a1.5 1.5 0 0 0-1.5-1.5H6A1.5 1.5 0 0 0 4.5 6v8A1.5 1.5 0 0 0 6 15.5h2.5"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2.5v2M12 19.5v2M4.6 4.6 6 6M18 18l1.4 1.4M2.5 12h2M19.5 12h2M4.6 19.4 6 18M18 6l1.4-1.4"/>',
  moon: '<path d="M19.5 14.6A8 8 0 0 1 9.4 4.5a8 8 0 1 0 10.1 10.1z"/>',
  close: '<path d="M6.5 6.5l11 11M17.5 6.5l-11 11"/>',
  chevron: '<path d="m9 6 6 6-6 6"/>',
  arrow: '<path d="M5 12h14M13 6l6 6-6 6"/>',
  clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>',
  db: '<ellipse cx="12" cy="6" rx="7.5" ry="2.8"/><path d="M4.5 6v12c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8V6M4.5 12c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8"/>',
  play: '<path d="M8 5.5v13l10.5-6.5z"/>',
  zoomIn: '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2M8.5 11h5M11 8.5v5"/>',
  zoomOut: '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2M8.5 11h5"/>',
  frame: '<path d="M4.5 9V6a1.5 1.5 0 0 1 1.5-1.5h3M15 4.5h3A1.5 1.5 0 0 1 19.5 6v3M19.5 15v3a1.5 1.5 0 0 1-1.5 1.5h-3M9 19.5H6A1.5 1.5 0 0 1 4.5 18v-3"/>',
  lease: '<path d="M4.5 8.5A1.5 1.5 0 0 1 6 7h2l1.5-2h5L16 7h2a1.5 1.5 0 0 1 1.5 1.5v9A1.5 1.5 0 0 1 18 19H6a1.5 1.5 0 0 1-1.5-1.5z"/><circle cx="12" cy="12.5" r="3.2"/>',
  info: '<circle cx="12" cy="12" r="8.5"/><path d="M12 11v5M12 7.8v.2"/>',
  pin: '<path d="M12 21s-6.5-6.2-6.5-11a6.5 6.5 0 0 1 13 0c0 4.8-6.5 11-6.5 11z"/><circle cx="12" cy="10" r="2.3"/>',
  table: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M9.5 9.5v10"/>',
  braces: '<path d="M8.5 3.5H7a2 2 0 0 0-2 2v4.2L3.5 12 5 14.3v4.2a2 2 0 0 0 2 2h1.5M15.5 3.5H17a2 2 0 0 1 2 2v4.2l1.5 2.3-1.5 2.3v4.2a2 2 0 0 1-2 2h-1.5"/>',
  index: '<path d="M4.5 6h15M4.5 12h10M4.5 18h6"/><circle cx="17.5" cy="16.5" r="2.5"/><path d="m19.4 18.4 1.6 1.6"/>',
  keyboard: '<rect x="2.5" y="6" width="19" height="12" rx="2"/><path d="M6 10h.01M9.5 10h.01M13 10h.01M16.5 10h.01M7 14h10"/>',
  theme: '<circle cx="12" cy="12" r="8.5"/><path d="M12 3.5v17a8.5 8.5 0 0 0 0-17z"/>',
};

export function icon(name, cls = '') {
  return svgEl(`<svg class="ico ${cls}" viewBox="0 0 24 24" aria-hidden="true" focusable="false">${ICONS[name] || ''}</svg>`);
}

export function iconButton(name, label, onClick, cls = '') {
  return h('button', { type: 'button', class: `icon-btn ${cls}`, 'aria-label': label, title: label, onClick }, icon(name));
}

export function button(label, onClick, { cls = '', iconName, title, type = 'button', disabled } = {}) {
  return h('button', { type, class: `btn ${cls}`, onClick, title, disabled }, iconName ? icon(iconName) : null, label);
}

export function badge(type) {
  const t = String(type || '');
  return h('span', { class: `badge t-${t}`, title: `${t} key` }, t || '?');
}

/* ---------------- formatting ---------------- */

const nf = new Intl.NumberFormat('en-US');

export function fmtInt(n) {
  if (n == null || n === '') return '–';
  if (typeof n === 'string' && !/^-?\d+$/.test(n)) return n;
  const x = typeof n === 'string' && n.length > 15 ? BigInt(n) : Number(n);
  return nf.format(x);
}

export function fmtNum(n, digits = 2) {
  const x = Number(n);
  if (!Number.isFinite(x)) return String(n);
  if (Number.isInteger(x)) return nf.format(x);
  return x.toLocaleString('en-US', { maximumFractionDigits: digits });
}

export function fmtBytes(n) {
  let x = Number(n);
  if (!Number.isFinite(x)) return '–';
  if (x < 1024) return `${x} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let i = -1;
  do { x /= 1024; i++; } while (x >= 1024 && i < units.length - 1);
  return `${x < 10 ? x.toFixed(2) : x < 100 ? x.toFixed(1) : Math.round(x)} ${units[i]}`;
}

export function fmtDuration(ms) {
  const x = Number(ms);
  if (!Number.isFinite(x)) return '–';
  if (x < 1000) return `${Math.max(0, Math.round(x))} ms`;
  let s = Math.floor(x / 1000);
  const d = Math.floor(s / 86400); s %= 86400;
  const hh = Math.floor(s / 3600); s %= 3600;
  const m = Math.floor(s / 60); s %= 60;
  if (d) return `${d}d ${hh}h`;
  if (hh) return `${hh}h ${m}m`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

export function fmtMs(ms) {
  const x = Number(ms);
  if (!Number.isFinite(x)) return '';
  if (x < 1) return `${x.toFixed(2)} ms`;
  if (x < 100) return `${x.toFixed(1)} ms`;
  if (x < 10000) return `${Math.round(x)} ms`;
  return `${(x / 1000).toFixed(1)} s`;
}

export function fmtTime(ms) {
  return new Date(Number(ms)).toLocaleTimeString([], { hour12: false });
}

export function fmtDateTime(ms) {
  return new Date(Number(ms)).toLocaleString([], { hour12: false });
}

export function plural(n, one, many = `${one}s`) {
  return `${fmtInt(n)} ${Number(n) === 1 ? one : many}`;
}

export function debounce(fn, ms) {
  let t = 0;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

/* ---------------- API ---------------- */

export class ApiError extends Error {
  constructor(message, info = {}) {
    super(message);
    this.name = 'ApiError';
    Object.assign(this, info);
  }
}

// ReplyError is an error reply from NilDB, such as "WRONGTYPE ...".
export class ReplyError extends Error {
  constructor(text) {
    super(text);
    this.name = 'ReplyError';
    this.code = text.split(' ', 1)[0];
  }
}

async function call(method, path, body, signal) {
  let res;
  try {
    res = await fetch(path, {
      method,
      signal,
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    if (e.name === 'AbortError') throw e;
    throw new ApiError('nildb-ui is not reachable; is it still running?', { status: 0, offline: true });
  }
  if (res.status === 204) return null;
  let data = null;
  try { data = await res.json(); } catch { /* not JSON */ }
  if (!res.ok) {
    throw new ApiError((data && data.error) || `HTTP ${res.status}`, {
      status: res.status, blocked: !!(data && data.blocked), conn: !!(data && data.conn),
    });
  }
  return data;
}

// toArg turns a string, number, byte array or key object into an
// argument the API accepts.
export function toArg(a) {
  if (typeof a === 'string') return a;
  if (typeof a === 'number' || typeof a === 'bigint') return String(a);
  if (a instanceof Uint8Array) return bytesArg(a);
  if (a && typeof a === 'object') {
    if (a.b64) return { b64: a.b64 };
    if ('k' in a) return a.k;
  }
  return String(a);
}

export const api = {
  db: 0,
  status: () => call('GET', '/api/status'),
  commands: () => call('GET', '/api/commands'),
  async run(args, { db = api.db, signal } = {}) {
    const r = await call('POST', '/api/exec', { args: args.map(toArg), db }, signal);
    return r.reply;
  },
  // ok runs a command and throws ReplyError when NilDB answers an error.
  async ok(args, opts) {
    return check(await api.run(args, opts));
  },
  async batch(cmds, { db = api.db, signal } = {}) {
    const r = await call('POST', '/api/exec', { cmds: cmds.map((c) => c.map(toArg)), db }, signal);
    return r.replies;
  },
  // atomic runs cmds inside MULTI/EXEC and throws when EXEC fails.
  async atomic(cmds, { db = api.db, signal } = {}) {
    const r = await call('POST', '/api/exec', { cmds: cmds.map((c) => c.map(toArg)), db, atomic: true }, signal);
    if (isErr(r.exec)) {
      const why = r.replies.find(isErr);
      throw new ReplyError(why ? why.v : r.exec.v);
    }
    const bad = r.replies.find(isErr);
    if (bad) throw new ReplyError(bad.v);
    return r.replies;
  },
  session: (session, args, db) => call('POST', '/api/exec', { args: args.map(toArg), db, session }),
  scan: (req, signal) => call('POST', '/api/scan', req, signal),
  closeSession(session) {
    fetch('/api/session/close', {
      method: 'POST', keepalive: true, headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session }),
    }).catch(() => {});
  },
};

/* ---------------- reply values ---------------- */

export const isErr = (v) => !!v && v.t === 'error';

export function check(v) {
  if (isErr(v)) throw new ReplyError(v.v);
  return v;
}

// text returns a scalar reply as a string; binary bulk strings come back
// with \xHH escapes.
export function text(v) {
  if (!v) return '';
  switch (v.t) {
    case 'bulk':
    case 'verbatim':
      return v.b64 != null ? escapeBytes(b64Bytes(v.b64)) : v.v;
    case 'simple': case 'error': case 'double': case 'big':
      return v.v;
    case 'int':
      return String(v.v);
    case 'bool':
      return v.v ? 'true' : 'false';
    default:
      return '';
  }
}

export function num(v) {
  if (!v) return NaN;
  if (v.t === 'int') return Number(v.v);
  if (v.t === 'double' || v.t === 'bulk' || v.t === 'simple' || v.t === 'big') {
    if (v.v === 'inf') return Infinity;
    if (v.v === '-inf') return -Infinity;
    return Number(v.v);
  }
  return NaN;
}

export function items(v) {
  return v && (v.t === 'array' || v.t === 'set' || v.t === 'push') ? v.v : [];
}

// entries returns the pairs of a map, or of a flat RESP2 key/value array.
export function entries(v) {
  if (!v) return [];
  if (v.t === 'map') return v.v;
  if (v.t === 'array') {
    const out = [];
    for (let i = 0; i + 1 < v.v.length; i += 2) out.push([v.v[i], v.v[i + 1]]);
    return out;
  }
  return [];
}

export function field(v, name) {
  for (const [k, x] of entries(v)) if (text(k) === name) return x;
  return undefined;
}

// toJS converts a reply to plain values: maps become objects, integers
// beyond 2^53 stay strings.
export function toJS(v) {
  switch (v?.t) {
    case 'map': {
      const o = {};
      for (const [k, x] of v.v) o[text(k)] = toJS(x);
      return o;
    }
    case 'array': case 'set': case 'push':
      return v.v.map(toJS);
    case 'int':
      return v.v;
    case 'double':
      return num(v);
    case 'bool':
      return v.v;
    case 'null':
      return null;
    default:
      return text(v);
  }
}

/* ---------------- bytes and quoting ---------------- */

export function b64Bytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function bytesB64(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();

// bytesArg sends bytes as text when they are UTF-8 and as base64 otherwise.
export function bytesArg(bytes) {
  try {
    return utf8.decode(bytes);
  } catch {
    return { b64: bytesB64(bytes) };
  }
}

// bulkBytes returns the bytes of a bulk reply.
export function bulkBytes(v) {
  return v.b64 != null ? b64Bytes(v.b64) : encoder.encode(v.v || '');
}

const escapes = { 0x5c: '\\\\', 0x22: '\\"', 0x0a: '\\n', 0x0d: '\\r', 0x09: '\\t', 0x07: '\\a', 0x08: '\\b' };
const hex2 = (c) => '\\x' + c.toString(16).padStart(2, '0');

// escapeBytes writes bytes the way redis-cli's sdscatrepr does, without
// the surrounding quotes.
export function escapeBytes(bytes) {
  let s = '';
  for (const c of bytes) s += escapes[c] || (c >= 0x20 && c < 0x7f ? String.fromCharCode(c) : hex2(c));
  return s;
}

// escapeText is escapeBytes for UTF-8 text: printable characters stay,
// control characters are escaped.
export function escapeText(str) {
  let s = '';
  for (const ch of str) {
    const c = ch.codePointAt(0);
    s += escapes[c] || (c < 0x20 || c === 0x7f ? hex2(c) : ch);
  }
  return s;
}

// reprBulk is a bulk reply as redis-cli prints it: quoted and escaped.
export function reprBulk(v) {
  return '"' + (v.b64 != null ? escapeBytes(b64Bytes(v.b64)) : escapeText(v.v)) + '"';
}

// quoteArg quotes an argument for display when redis-cli would need it.
export function quoteArg(a) {
  const s = typeof a === 'string' ? a : a && a.b64 ? escapeBytes(b64Bytes(a.b64)) : String(a);
  if (s !== '' && /^[^\s"'\\]+$/.test(s) && !/[\x00-\x1f\x7f]/.test(s)) return s;
  // Single quotes keep JSON readable; inside them only \' is an escape, so a
  // trailing backslash would escape the closing quote.
  if (s.includes('"') && !s.includes("'") && !s.endsWith('\\') && !/[\x00-\x1f\x7f]/.test(s)) return `'${s}'`;
  return '"' + escapeText(s) + '"';
}

const isSpace = (c) => c === 0x20 || (c >= 0x09 && c <= 0x0d);
const isHex = (c) => (c >= 0x30 && c <= 0x39) || (c >= 0x61 && c <= 0x66) || (c >= 0x41 && c <= 0x46);
const hexVal = (c) => (c >= 0x61 ? c - 0x57 : c >= 0x41 ? c - 0x37 : c - 0x30);
const quoteEscapes = { 0x6e: 0x0a, 0x72: 0x0d, 0x74: 0x09, 0x62: 0x08, 0x61: 0x07 };

// splitCommands splits Console input into commands and arguments with the
// rules of sdssplitargs, which redis-cli uses for its prompt: double quotes
// take \xHH \n \r \t \a \b and \<c>, single quotes take \', a closing quote
// must be followed by whitespace. A newline outside quotes ends a command.
// Arguments are byte arrays, so "\xff" is the single byte 0xff.
export function splitCommands(input) {
  const line = encoder.encode(input);
  const n = line.length;
  const cmds = [];
  let argv = [];
  let p = 0;
  for (;;) {
    while (p < n && isSpace(line[p])) {
      if (line[p] === 0x0a && argv.length) {
        cmds.push(argv);
        argv = [];
      }
      p++;
    }
    if (p >= n) {
      if (argv.length) cmds.push(argv);
      return { cmds };
    }
    const cur = [];
    let inq = false;
    let insq = false;
    let done = false;
    while (!done) {
      if (inq) {
        if (p + 3 < n && line[p] === 0x5c && line[p + 1] === 0x78 && isHex(line[p + 2]) && isHex(line[p + 3])) {
          cur.push((hexVal(line[p + 2]) << 4) | hexVal(line[p + 3]));
          p += 3;
        } else if (p + 1 < n && line[p] === 0x5c) {
          p++;
          cur.push(quoteEscapes[line[p]] ?? line[p]);
        } else if (p < n && line[p] === 0x22) {
          if (p + 1 < n && !isSpace(line[p + 1])) return { error: 'a closing quote must be followed by a space' };
          done = true;
        } else if (p >= n) {
          return { error: 'unbalanced quotes' };
        } else {
          cur.push(line[p]);
        }
      } else if (insq) {
        if (p + 1 < n && line[p] === 0x5c && line[p + 1] === 0x27) {
          p++;
          cur.push(0x27);
        } else if (p < n && line[p] === 0x27) {
          if (p + 1 < n && !isSpace(line[p + 1])) return { error: 'a closing quote must be followed by a space' };
          done = true;
        } else if (p >= n) {
          return { error: 'unbalanced quotes' };
        } else {
          cur.push(line[p]);
        }
      } else {
        if (p >= n) break;
        const c = line[p];
        if (c === 0x0a) break;
        if (c === 0x20 || c === 0x0d || c === 0x09) done = true;
        else if (c === 0x22) inq = true;
        else if (c === 0x27) insq = true;
        else cur.push(c);
      }
      if (p < n) p++;
    }
    argv.push(Uint8Array.from(cur));
  }
}

/* ---------------- keys ---------------- */

// A key from /api/scan is {k, b64?}: b64 holds the bytes of a name that
// is not UTF-8.
export const keyArg = (k) => (k.b64 ? { b64: k.b64 } : k.k);
export const keyName = (k) => (k.b64 ? escapeBytes(b64Bytes(k.b64)) : escapeText(k.k));
export const keyId = (k) => (k.b64 ? 'b:' + k.b64 : 's:' + k.k);
export function keyFromReply(v) {
  return v.b64 != null ? { k: text(v), b64: v.b64 } : { k: v.v };
}

/* ---------------- INFO ---------------- */

// parseInfo turns INFO text into {section: {field: value}}.
export function parseInfo(txt) {
  const out = {};
  let cur = null;
  for (const line of String(txt).split(/\r?\n/)) {
    if (!line) continue;
    if (line.startsWith('# ')) {
      cur = line.slice(2).trim().toLowerCase();
      out[cur] = out[cur] || {};
      continue;
    }
    const i = line.indexOf(':');
    if (i > 0 && cur) out[cur][line.slice(0, i)] = line.slice(i + 1);
  }
  return out;
}

// parseKV turns "keys=1,expires=0" into {keys: "1", expires: "0"}.
export function parseKV(s) {
  const o = {};
  for (const part of String(s || '').split(',')) {
    const i = part.indexOf('=');
    if (i > 0) o[part.slice(0, i)] = part.slice(i + 1);
  }
  return o;
}

/* ---------------- Extended JSON ---------------- */

// ejsonKind names the BSON type behind a canonical Extended JSON value.
export function ejsonKind(v) {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  switch (typeof v) {
    case 'string': return 'string';
    case 'boolean': return 'bool';
    case 'number': return 'number';
  }
  const ks = Object.keys(v);
  if (ks.length === 1 || (ks.length === 2 && ks[0] === '$code')) {
    switch (ks[0]) {
      case '$oid': return 'oid';
      case '$date': return 'date';
      case '$numberInt': return 'int';
      case '$numberLong': return 'long';
      case '$numberDouble': return 'double';
      case '$numberDecimal': return 'decimal';
      case '$binary': return 'binary';
      case '$regularExpression': return 'regex';
      case '$timestamp': return 'timestamp';
      case '$minKey': return 'minkey';
      case '$maxKey': return 'maxkey';
      case '$undefined': return 'undefined';
      case '$symbol': return 'symbol';
      case '$code': return 'code';
      case '$dbPointer': return 'dbpointer';
    }
  }
  return 'object';
}

export function ejsonDateMs(v) {
  const d = v.$date;
  if (typeof d === 'string') return Date.parse(d);
  if (d && typeof d === 'object' && d.$numberLong != null) return Number(d.$numberLong);
  return Number(d);
}

function isoDate(ms) {
  const d = new Date(ms);
  return Number.isNaN(d.getTime()) ? String(ms) : d.toISOString();
}

// ejsonNumber returns the number behind an int, long, double or decimal,
// or NaN.
export function ejsonNumber(v) {
  switch (ejsonKind(v)) {
    case 'int': return Number(v.$numberInt);
    case 'long': return Number(v.$numberLong);
    case 'double': return Number(v.$numberDouble);
    case 'decimal': return Number(v.$numberDecimal);
    case 'number': return v;
  }
  return NaN;
}

// ejsonLabel is a short display form of a value, as in mongosh.
export function ejsonLabel(v) {
  switch (ejsonKind(v)) {
    case 'null': return 'null';
    case 'string': return v;
    case 'bool': case 'number': return String(v);
    case 'oid': return `ObjectId("${v.$oid}")`;
    case 'date': return isoDate(ejsonDateMs(v));
    case 'int': return v.$numberInt;
    case 'long': return v.$numberLong;
    case 'double': return v.$numberDouble;
    case 'decimal': return v.$numberDecimal;
    case 'binary': return `Binary(${v.$binary.subType}, ${Math.floor((v.$binary.base64.length * 3) / 4)} bytes)`;
    case 'regex': return `/${v.$regularExpression.pattern}/${v.$regularExpression.options}`;
    case 'timestamp': return `Timestamp(${v.$timestamp.t}, ${v.$timestamp.i})`;
    case 'minkey': return 'MinKey';
    case 'maxkey': return 'MaxKey';
    case 'undefined': return 'undefined';
    case 'array': return `[${v.length}]`;
    case 'object': {
      const ks = Object.keys(v);
      return ks.length ? `{ ${ks.slice(0, 4).join(', ')}${ks.length > 4 ? ', …' : ''} }` : '{}';
    }
  }
  return JSON.stringify(v);
}

// ejsonPlain converts to plain JavaScript for tables and charts: numbers
// for numeric types, ISO strings for dates, hex for ObjectIds.
export function ejsonPlain(v) {
  const kind = ejsonKind(v);
  switch (kind) {
    case 'int': case 'long': case 'double': case 'decimal': return ejsonNumber(v);
    case 'date': return isoDate(ejsonDateMs(v));
    case 'oid': return v.$oid;
    case 'array': return v.map(ejsonPlain);
    case 'object': {
      const o = {};
      for (const [k, x] of Object.entries(v)) o[k] = ejsonPlain(x);
      return o;
    }
    case 'null': case 'string': case 'bool': case 'number': return v;
  }
  return ejsonLabel(v);
}

const INT32_MIN = -2147483648n;
const INT32_MAX = 2147483647n;
const MAX_ISO_MS = 253402300799999;

// ejsonEdit renders a document as relaxed Extended JSON that parses back to
// the same BSON types: int32 as a plain integer, int64 as a plain integer
// only when it does not fit int32, doubles always with a fraction or an
// exponent, dates as ISO strings between 1970 and 9999.
export function ejsonEdit(v, indent = '') {
  const kind = ejsonKind(v);
  switch (kind) {
    case 'int':
      return v.$numberInt;
    case 'long': {
      const n = BigInt(v.$numberLong);
      return n < INT32_MIN || n > INT32_MAX ? v.$numberLong : JSON.stringify(v);
    }
    case 'double': {
      const x = Number(v.$numberDouble);
      if (!Number.isFinite(x)) return JSON.stringify(v);
      if (Object.is(x, -0)) return '-0.0';
      const s = String(x);
      return /[.e]/.test(s) ? s : s + '.0';
    }
    case 'date': {
      const ms = ejsonDateMs(v);
      return ms >= 0 && ms <= MAX_ISO_MS ? `{"$date": "${isoDate(ms)}"}` : JSON.stringify(v);
    }
    case 'array': {
      if (!v.length) return '[]';
      const parts = v.map((x) => ejsonEdit(x, indent + '  '));
      const flat = `[${parts.join(', ')}]`;
      if (flat.length <= 72 && !flat.includes('\n')) return flat;
      return `[\n${parts.map((p) => indent + '  ' + p).join(',\n')}\n${indent}]`;
    }
    case 'object': {
      const ks = Object.keys(v);
      if (!ks.length) return '{}';
      const parts = ks.map((k) => `${indent}  ${JSON.stringify(k)}: ${ejsonEdit(v[k], indent + '  ')}`);
      return `{\n${parts.join(',\n')}\n${indent}}`;
    }
    default:
      return JSON.stringify(v);
  }
}

// parseJSONArg checks that a filter, projection or document is JSON and
// returns it trimmed, or throws with the parser's message.
export function parseJSONArg(txt, what, { array = false } = {}) {
  const s = String(txt).trim();
  if (!s) return array ? '[]' : '{}';
  let v;
  try {
    v = JSON.parse(s);
  } catch (e) {
    throw new Error(`${what} is not valid JSON: ${e.message}`);
  }
  if (array ? !Array.isArray(v) : v === null || typeof v !== 'object' || Array.isArray(v)) {
    throw new Error(`${what} must be a JSON ${array ? 'array' : 'object'}`);
  }
  return s;
}

/* ---------------- misc ---------------- */

export function uid(prefix = 'id') {
  return `${prefix}-${Math.random().toString(36).slice(2, 9)}`;
}

export async function copyText(s) {
  await navigator.clipboard.writeText(s);
}

// store reads and writes localStorage without throwing.
export const store = {
  get(k, dflt) {
    try {
      const v = localStorage.getItem('nildb-ui.' + k);
      return v == null ? dflt : JSON.parse(v);
    } catch {
      return dflt;
    }
  },
  set(k, v) {
    try { localStorage.setItem('nildb-ui.' + k, JSON.stringify(v)); } catch { /* full or disabled */ }
  },
};
