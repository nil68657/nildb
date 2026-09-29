// Console: a REPL on one NilDB connection per tab, so MULTI, WATCH,
// SELECT and HELLO behave as in redis-cli. Input is split with redis-cli's
// quoting rules; replies print in redis-cli's RESP3 layout, with a JSON
// tree for replies that carry Extended JSON documents.

import {
  h, clear, api, splitCommands, bytesArg, quoteArg, reprBulk, fmtMs, fmtInt, button, store, isErr,
  text, escapeText,
} from '../lib.js';
import { jsonTree, errorText, spinner } from '../ui.js';

const MAX_ENTRIES = 400;
const MAX_ELEMS = 1000;
const MAX_LINES = 4000;
const HISTORY = 500;

const EMPTY = { array: '(empty array)', set: '(empty set)', map: '(empty hash)', push: '(empty push)' };

// format lays a reply out the way redis-cli's cliFormatReplyTTY does. It
// returns lines, each a list of [text, class] runs.
function format(v, prefix = '') {
  if (v.attrs && v.attrs.length) {
    const { attrs, ...plain } = v;
    return [[['(attributes)', 'r-meta']], ...format({ t: 'map', v: attrs }, prefix), ...format(plain, prefix)];
  }
  switch (v.t) {
    case 'error': return [[[`(error) ${v.v}`, 'r-err']]];
    case 'simple': return [[[v.v, 'r-status']]];
    case 'int': return [[[`(integer) ${v.v}`, 'r-num']]];
    case 'double': return [[[`(double) ${v.v}`, 'r-num']]];
    case 'big': return [[[`(big number) ${v.v}`, 'r-num']]];
    case 'bool': return [[[v.v ? '(true)' : '(false)', 'r-bool']]];
    case 'null': return [[['(nil)', 'r-nil']]];
    case 'bulk': return [[[reprBulk(v), 'r-str']]];
    case 'verbatim': {
      const lines = text(v).split('\n');
      if (lines.length > 1 && lines[lines.length - 1] === '') lines.pop();
      return lines.map((l) => [[l.replace(/\r$/, ''), '']]);
    }
    case 'array': case 'set': case 'push': case 'map': {
      const n = v.v.length;
      if (!n) return [[[EMPTY[v.t], 'r-meta']]];
      const idxlen = String(n).length;
      const sep = v.t === 'set' ? '~' : v.t === 'map' ? '#' : ')';
      const child = prefix + ' '.repeat(idxlen + 2);
      const out = [];
      const limit = Math.min(n, MAX_ELEMS);
      let i = 0;
      while (i < limit && out.length <= MAX_LINES) {
        const lead = (i === 0 ? '' : prefix) + String(i + 1).padStart(idxlen) + sep + ' ';
        const elem = v.t === 'map' ? v.v[i][0] : v.v[i];
        const lines = format(elem, child);
        lines[0].unshift([lead, 'r-idx']);
        if (v.t === 'map') {
          const val = format(v.v[i][1], child);
          lines[lines.length - 1].push([' => ', 'r-idx'], ...val[0]);
          lines.push(...val.slice(1));
        }
        out.push(...lines);
        i++;
      }
      if (n > i) out.push([[`${prefix}… ${fmtInt(n - i)} more elements not shown`, 'r-meta']]);
      return out;
    }
  }
  return [[[JSON.stringify(v), '']]];
}

function paint(lines) {
  const frag = document.createDocumentFragment();
  const n = Math.min(lines.length, MAX_LINES);
  for (let i = 0; i < n; i++) {
    for (const [txt, cls] of lines[i]) frag.append(cls ? h('span', { class: cls }, txt) : txt);
    if (i < n - 1) frag.append('\n');
  }
  if (lines.length > MAX_LINES) frag.append('\n', h('span', { class: 'r-meta' }, `… ${fmtInt(lines.length - MAX_LINES)} more lines not shown`));
  return frag;
}

// jsonish converts a reply to plain values for the tree view, parsing
// bulk strings that hold JSON documents, as DOC.* replies do.
function jsonish(v) {
  switch (v.t) {
    case 'map': {
      const o = {};
      for (const [k, x] of v.v) o[text(k)] = jsonish(x);
      return o;
    }
    case 'array': case 'set': case 'push': return v.v.map(jsonish);
    case 'bulk': {
      const s = v.v;
      if (s && (s[0] === '{' || s[0] === '[')) {
        try { return JSON.parse(s); } catch { /* plain text */ }
      }
      return v.b64 != null ? text(v) : s;
    }
    case 'int': return typeof v.v === 'string' ? { $numberLong: v.v } : v.v;
    case 'double': return Number(v.v);
    case 'bool': return v.v;
    case 'null': return null;
    default: return text(v);
  }
}

function hasJSON(v) {
  if (v.t === 'bulk') return !!v.v && (v.v[0] === '{' || v.v[0] === '[');
  if (v.t === 'map') return v.v.some(([k, x]) => hasJSON(k) || hasJSON(x));
  if (v.t === 'array' || v.t === 'set' || v.t === 'push') return v.v.some(hasJSON);
  return false;
}

export function create(app) {
  const out = h('div', { class: 'console-out', role: 'log', 'aria-label': 'Console output', tabindex: 0 });
  const promptEl = h('span', { class: 'prompt', 'aria-hidden': 'true' });
  const input = h('textarea', {
    rows: 1, 'aria-label': 'Command', spellcheck: false, autocomplete: 'off', autocapitalize: 'off',
    placeholder: 'Type a command, such as INFO server or HSCAN user:1 0',
  });
  const hint = h('div', { class: 'hint', 'aria-live': 'polite' });
  const sessionNote = h('span', { class: 'sub' });
  const el = h('section', { class: 'view', 'aria-label': 'Console' },
    h('div', { class: 'view-head' }, h('h1', null, 'Console'), sessionNote, h('div', { class: 'grow' }),
      button('Clear', () => clearOut(), { cls: 'sm ghost', title: 'Clear the output (Ctrl L)' })),
    h('div', { class: 'console' }, out, h('div', { class: 'console-in' }, promptEl, input), hint));

  let history = store.get('history', []);
  let hIdx = history.length;
  let draft = '';
  let state = { db: app.db, multi: false, proto: 3 };
  let queue = Promise.resolve();
  let tab = null;

  const addr = () => app.status?.nildb || 'nildb';
  function prompt() {
    const db = state.multi ? state.db : app.db;
    return `${addr()}${db > 0 ? `[${db}]` : ''}${state.multi ? '(TX)' : ''}>`;
  }
  function updatePrompt() {
    promptEl.textContent = prompt();
    promptEl.classList.toggle('multi', state.multi);
    sessionNote.textContent = `one connection for this tab · RESP${state.proto} · db ${state.multi ? state.db : app.db}${state.multi ? ' · inside MULTI' : ''}${app.readonly ? ' · read-only' : ''}`;
  }

  function autosize() {
    input.style.height = 'auto';
    input.style.height = `${Math.min(input.scrollHeight, window.innerHeight * 0.4)}px`;
  }

  const nearBottom = () => out.scrollHeight - out.scrollTop - out.clientHeight < 40;

  function addEntry(node) {
    const stick = nearBottom();
    out.append(node);
    while (out.children.length > MAX_ENTRIES) out.firstElementChild.remove();
    if (stick) out.scrollTop = out.scrollHeight;
  }

  function note(msg) {
    addEntry(h('div', { class: 'entry' }, h('div', { class: 'entry-note' }, msg)));
  }

  function clearOut() {
    out.replaceChildren();
  }

  function commandLine(argv) {
    const parts = argv.map((a) => quoteArg(bytesArg(a)));
    return h('div', { class: 'entry-cmd' },
      h('span', { class: 'prompt' }, prompt()),
      h('span', { class: 'cmdtext' }, h('span', { class: 'cmdname' }, parts[0]), parts.length > 1 ? ' ' + parts.slice(1).join(' ') : ''));
  }

  function printReply(entry, reply, ms) {
    const head = entry.querySelector('.entry-cmd');
    const pre = h('pre', { class: 'entry-reply' }, paint(format(reply)));
    head.append(h('span', { class: 'ms' }, fmtMs(ms)));
    if (hasJSON(reply)) {
      let tree = null;
      const toggle = button('Tree', () => {
        if (!tree) tree = jsonTree(jsonish(reply), { depth: 3 });
        const toTree = pre.isConnected;
        if (toTree) pre.replaceWith(tree);
        else tree.replaceWith(pre);
        toggle.textContent = toTree ? 'Text' : 'Tree';
      }, { cls: 'sm ghost', title: 'Show the reply as a JSON tree' });
      head.append(toggle);
    }
    entry.append(pre);
  }

  function help(argv) {
    const name = argv[1] ? new TextDecoder().decode(argv[1]).toLowerCase() : '';
    if (name) {
      const c = app.command(name);
      if (!c) {
        note(`NilDB has no command named ${name.toUpperCase()}.`);
        return;
      }
      const lines = [
        `${c.name.toUpperCase()}  ${c.summary || ''}`,
        `  group ${c.group || 'generic'} · since ${c.since || '?'} · arity ${c.arity} · flags ${c.flags.join(', ') || 'none'}${c.blocked ? ' · refused by --readonly' : ''}`,
      ];
      for (const s of c.subcommands || []) lines.push(`  ${s.name.replace('|', ' ').toUpperCase()}  ${s.summary || ''}${s.blocked ? ' (refused by --readonly)' : ''}`);
      addEntry(h('div', { class: 'entry' }, h('pre', { class: 'entry-reply' }, lines.join('\n'))));
      return;
    }
    addEntry(h('div', { class: 'entry' }, h('pre', { class: 'entry-reply' }, [
      'Local commands, not sent to NilDB:',
      '  help            this text',
      '  help <command>  summary, arity and flags from COMMAND DOCS',
      '  clear           clear the output (Ctrl L)',
      '',
      'Keys: Enter runs, Shift Enter adds a line, ↑ and ↓ walk the history, Tab completes a command, Esc clears the input.',
      'Quoting follows redis-cli: "a b" and \'a b\' are one argument; "\\x00" is a byte; a newline outside quotes starts the next command.',
      `NilDB lists ${fmtInt(app.commands.length)} commands; COMMAND DOCS has their summaries.`,
    ].join('\n'))));
  }

  async function runOne(argv) {
    const name = new TextDecoder().decode(argv[0]).toLowerCase();
    if (name === 'clear') {
      clearOut();
      return;
    }
    if (name === 'help' || name === '?') {
      help(argv);
      return;
    }
    const entry = h('div', { class: 'entry' }, commandLine(argv));
    const wait = spinner('Waiting for NilDB');
    entry.querySelector('.entry-cmd').append(wait);
    addEntry(entry);
    try {
      const r = await api.session(app.session, argv, app.db);
      wait.remove();
      printReply(entry, r.reply, r.ms);
      state = r.session;
      if (!state.multi && state.db >= 0 && state.db !== app.db) app.setDB(state.db);
      if (name === 'quit' && !isErr(r.reply)) note('The connection closed; the next command opens a new one.');
    } catch (e) {
      wait.remove();
      entry.append(h('pre', { class: 'entry-reply' }, h('span', { class: 'r-err' }, e.blocked ? `(refused) ${e.message}` : `(nildb-ui) ${errorText(e)}`)));
      if (!e.blocked) state = { db: app.db, multi: false, proto: 3 };
    }
    updatePrompt();
    if (nearBottom()) out.scrollTop = out.scrollHeight;
  }

  function submit() {
    const src = input.value;
    if (!src.trim()) return;
    if (history[history.length - 1] !== src) {
      history.push(src);
      if (history.length > HISTORY) history = history.slice(-HISTORY);
      store.set('history', history);
    }
    hIdx = history.length;
    draft = '';
    input.value = '';
    autosize();
    updateHint();
    const { cmds, error } = splitCommands(src);
    if (error) {
      addEntry(h('div', { class: 'entry' },
        h('div', { class: 'entry-cmd' }, h('span', { class: 'prompt' }, prompt()), h('span', { class: 'cmdtext' }, escapeText(src))),
        h('pre', { class: 'entry-reply' }, h('span', { class: 'r-err' }, `(error) Invalid argument(s): ${error}`))));
      return;
    }
    for (const argv of cmds) queue = queue.then(() => runOne(argv));
  }

  /* ---------------- hints and completion ---------------- */

  function firstWord() {
    const m = input.value.match(/^\s*(\S+)(\s+(\S*))?/);
    return m ? { cmd: m[1], hasSpace: !!m[2], sub: m[3] || '' } : null;
  }

  function updateHint() {
    const w = firstWord();
    if (!w) {
      clear(hint, 'Enter runs · Shift Enter adds a line · ↑ ↓ history · Tab completes · help lists local commands');
      return;
    }
    const lower = w.cmd.toLowerCase();
    if (lower === 'help' || lower === 'clear') {
      clear(hint, h('b', null, w.cmd.toUpperCase()), ' runs in the browser; NilDB never sees it');
      return;
    }
    let c = app.command(lower);
    if (c && c.subcommands?.length && w.sub) c = app.command(`${lower}|${w.sub.toLowerCase()}`) || c;
    if (!c) {
      clear(hint, w.hasSpace ? `NilDB has no command named ${w.cmd.toUpperCase()}` : '');
      return;
    }
    const subs = c.subcommands?.length ? ` · subcommands ${c.subcommands.map((s) => s.name.split('|')[1].toUpperCase()).join(' ')}` : '';
    clear(hint, h('b', null, c.name.replace('|', ' ').toUpperCase()), ` ${c.summary || ''} · arity ${c.arity}${c.flags.length ? ' · ' + c.flags.join(', ') : ''}${subs}`,
      c.blocked ? h('span', { class: 'r-err' }, ' · refused by --readonly') : '');
  }

  function complete() {
    const v = input.value;
    const m = v.match(/^(\s*)(\S*)(\s+)?(\S*)$/);
    if (!m) return false;
    const [, lead, word, space, second] = m;
    let pool;
    let prefix;
    let build;
    if (!space) {
      pool = app.commands.map((c) => c.name.toUpperCase());
      prefix = word.toUpperCase();
      build = (x) => `${lead}${x} `;
    } else {
      const c = app.command(word.toLowerCase());
      if (!c || !c.subcommands?.length) return false;
      pool = c.subcommands.map((s) => s.name.split('|')[1].toUpperCase());
      prefix = second.toUpperCase();
      build = (x) => `${lead}${word}${space}${x} `;
    }
    if (!tab || tab.base !== v) {
      const hits = pool.filter((x) => x.startsWith(prefix)).sort();
      if (!hits.length) return false;
      tab = { hits, i: -1, base: null };
    }
    tab.i = (tab.i + 1) % tab.hits.length;
    input.value = build(tab.hits[tab.i]);
    tab.base = input.value;
    updateHint();
    return true;
  }

  input.addEventListener('keydown', (e) => {
    const mod = e.metaKey || e.ctrlKey;
    if (e.key === 'Enter' && !e.shiftKey && !e.altKey) {
      e.preventDefault();
      submit();
      return;
    }
    if (e.key === 'Tab' && !e.shiftKey && !mod) {
      if (complete()) e.preventDefault();
      return;
    }
    tab = null;
    if (mod && e.key.toLowerCase() === 'l') {
      e.preventDefault();
      clearOut();
      return;
    }
    if (e.key === 'Escape' && input.value) {
      e.preventDefault();
      input.value = '';
      autosize();
      updateHint();
      return;
    }
    const firstLine = !input.value.slice(0, input.selectionStart).includes('\n');
    const lastLine = !input.value.slice(input.selectionEnd).includes('\n');
    if (e.key === 'ArrowUp' && firstLine && !e.shiftKey && history.length) {
      e.preventDefault();
      if (hIdx === history.length) draft = input.value;
      hIdx = Math.max(0, hIdx - 1);
      input.value = history[hIdx];
      autosize();
      updateHint();
    } else if (e.key === 'ArrowDown' && lastLine && !e.shiftKey && hIdx < history.length) {
      e.preventDefault();
      hIdx++;
      input.value = hIdx === history.length ? draft : history[hIdx];
      autosize();
      updateHint();
    }
  });
  input.addEventListener('input', () => {
    autosize();
    updateHint();
  });
  out.addEventListener('mouseup', () => {
    if (!window.getSelection().toString()) input.focus();
  });

  app.on('db', updatePrompt);
  app.on('server', updateHint);

  let greeted = false;
  updatePrompt();
  updateHint();
  return {
    el,
    show() {
      if (!greeted) {
        greeted = true;
        note(`Connected to ${addr()} through nildb-ui. Commands from this tab share one connection, so MULTI, WATCH, SELECT and HELLO keep their effect. Type help for the local commands.`);
      }
      updatePrompt();
      requestAnimationFrame(() => input.focus());
    },
    focusSearch() { input.focus(); },
    prefill(s) {
      input.value = s;
      autosize();
      updateHint();
      input.focus();
      input.setSelectionRange(s.length, s.length);
    },
    clear: clearOut,
  };
}
