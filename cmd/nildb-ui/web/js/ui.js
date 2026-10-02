// Interface pieces shared by the views: toasts, dialogs, a virtualized
// list, the JSON tree, charts and the EXPLAIN plan.

import {
  h, append, clear, icon, svgEl, uid, fmtInt, fmtNum, plural, ejsonKind, ejsonLabel, ApiError,
} from './lib.js';

/* ---------------- toasts ---------------- */

export function errorText(e) {
  if (!e) return 'Unknown error';
  if (e instanceof ApiError && e.blocked) return e.message;
  return e.message || String(e);
}

export function toast(message, kind = 'info', ms = kind === 'err' ? 7000 : 3200) {
  const box = document.getElementById('toasts');
  while (box.children.length >= 4) box.firstElementChild.remove();
  const el = h('div', { class: `toast ${kind}` }, message);
  box.append(el);
  const close = () => {
    el.classList.add('out');
    setTimeout(() => el.remove(), 170);
  };
  el.addEventListener('click', close);
  setTimeout(close, ms);
}

export function toastError(e, prefix = '') {
  toast((prefix ? prefix + ': ' : '') + errorText(e), 'err');
}

/* ---------------- dialogs ---------------- */

function modal({ title, wide }) {
  const dlg = h('dialog', { class: `modal${wide ? ' wide' : ''}`, 'aria-labelledby': uid('dlg') });
  const head = h('div', { class: 'dialog-head' }, h('h3', { id: dlg.getAttribute('aria-labelledby') }, title));
  document.body.append(dlg);
  dlg.addEventListener('close', () => setTimeout(() => dlg.remove(), 200));
  return { dlg, head };
}

// confirmDialog asks before a destructive action. name, when given, is
// shown on its own line so the reader sees exactly what goes.
export function confirmDialog({ title, message, name, confirm = 'Delete', danger = true }) {
  return new Promise((resolve) => {
    const { dlg, head } = modal({ title });
    const cancel = h('button', { type: 'button', class: 'btn ghost', onClick: () => dlg.close('cancel') }, 'Cancel');
    const ok = h('button', { type: 'submit', class: `btn ${danger ? 'danger-solid' : 'primary'}` }, confirm);
    const form = h('form', { class: 'dialog-form', method: 'dialog' },
      head,
      h('div', { class: 'dialog-body' },
        message ? h('p', null, message) : null,
        name != null ? h('div', { class: 'dialog-name' }, name) : null),
      h('div', { class: 'dialog-foot' }, cancel, ok));
    form.addEventListener('submit', (e) => {
      e.preventDefault();
      dlg.close('ok');
    });
    dlg.append(form);
    dlg.addEventListener('close', () => resolve(dlg.returnValue === 'ok'));
    dlg.showModal();
    (danger ? cancel : ok).focus();
  });
}

function fieldEl(f) {
  const id = uid('f');
  let input;
  const common = { id, name: f.name, required: f.required, placeholder: f.placeholder, autocomplete: 'off', spellcheck: false };
  switch (f.type) {
    case 'textarea':
      input = h('textarea', { ...common, class: `textarea ${f.mono === false ? '' : 'mono'}`, rows: f.rows || 6 }, f.value ?? '');
      break;
    case 'select':
      input = h('select', { ...common, class: 'select' },
        (f.options || []).map(([v, label]) => h('option', { value: v, selected: String(v) === String(f.value) }, label)));
      break;
    case 'checkbox':
      input = h('input', { ...common, type: 'checkbox', checked: !!f.value });
      return h('label', { class: 'check' }, input, f.label);
    default:
      input = h('input', { ...common, class: `input ${f.mono ? 'mono' : ''}`, type: f.type || 'text', value: f.value ?? '', min: f.min, step: f.step });
  }
  if (f.autofocus) input.setAttribute('autofocus', '');
  return h('label', { class: 'field', for: id }, h('span', null, f.label), input, f.hint ? h('span', { class: 'field-hint' }, f.hint) : null);
}

// formDialog collects values and hands them to onSubmit. When onSubmit
// throws, the message shows in the dialog and it stays open; otherwise
// the dialog closes and the promise resolves to what onSubmit returned
// (or the values). Cancel resolves to null.
export function formDialog({ title, intro, fields, submit = 'Save', onSubmit, wide, danger }) {
  return new Promise((resolve) => {
    const { dlg, head } = modal({ title, wide });
    const err = h('div', { class: 'field-error', role: 'alert' });
    const cancel = h('button', { type: 'button', class: 'btn ghost', onClick: () => dlg.close() }, 'Cancel');
    const ok = h('button', { type: 'submit', class: `btn ${danger ? 'danger-solid' : 'primary'}` }, submit);
    const form = h('form', { class: 'dialog-form', method: 'dialog', novalidate: true },
      head,
      h('div', { class: 'dialog-body' }, intro ? h('p', null, intro) : null, fields.map(fieldEl), err),
      h('div', { class: 'dialog-foot' }, cancel, ok));
    let result = null;
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const values = {};
      for (const f of fields) {
        const el = form.elements.namedItem(f.name);
        values[f.name] = f.type === 'checkbox' ? el.checked : el.value;
      }
      err.textContent = '';
      ok.disabled = true;
      try {
        result = onSubmit ? await onSubmit(values) : values;
        if (result === undefined) result = values;
        dlg.close('ok');
      } catch (ex) {
        err.textContent = errorText(ex);
      } finally {
        ok.disabled = false;
      }
    });
    dlg.append(form);
    dlg.addEventListener('close', () => resolve(dlg.returnValue === 'ok' ? result : null));
    dlg.showModal();
  });
}

// infoDialog shows content with a single Close button.
export function infoDialog({ title, body, wide = true }) {
  const { dlg, head } = modal({ title, wide });
  const close = h('button', { type: 'submit', class: 'btn' }, 'Close');
  const form = h('form', { class: 'dialog-form', method: 'dialog' },
    head, h('div', { class: 'dialog-body' }, body), h('div', { class: 'dialog-foot' }, close));
  dlg.append(form);
  dlg.showModal();
  close.focus();
  return dlg;
}

/* ---------------- small controls ---------------- */

// seg is a segmented control; el.set(value) moves the selection.
export function seg(options, value, onChange, label) {
  const el = h('div', { class: 'seg', role: 'group', 'aria-label': label });
  const btns = options.map(([v, txt]) => h('button', {
    type: 'button', 'aria-pressed': String(v === value), onClick: () => { set(v); onChange(v); },
  }, txt));
  function set(v) {
    value = v;
    btns.forEach((b, i) => b.setAttribute('aria-pressed', String(options[i][0] === v)));
  }
  append(el, btns);
  el.set = set;
  el.get = () => value;
  return el;
}

export function spinner(label = 'Loading') {
  return h('span', { class: 'spinner', role: 'progressbar', 'aria-label': label });
}

export function emptyState(iconName, title, textLine) {
  return h('div', { class: 'empty' }, icon(iconName), h('strong', null, title), textLine ? h('span', null, textLine) : null);
}

export function searchInput(props) {
  return h('div', { class: 'search' }, icon('search'), h('input', { class: 'input', type: 'search', autocomplete: 'off', spellcheck: false, ...props }));
}

export function kv(pairs) {
  const dl = h('dl', { class: 'kv' });
  for (const [k, v] of pairs) {
    if (v === undefined) continue;
    dl.append(h('dt', null, k), h('dd', null, v == null || v === '' ? '–' : v));
  }
  return dl;
}

/* ---------------- virtual list ---------------- */

// VirtualList renders only the rows in view, so a list of 100,000 keys
// costs a few dozen DOM nodes. It is a listbox: arrows, Page Up/Down,
// Home/End move the selection, Enter opens.
export class VirtualList {
  constructor({ rowHeight = 34, render, label, onSelect, onOpen, onEnd, overscan = 8 }) {
    Object.assign(this, { rowHeight, render, onSelect, onOpen, onEnd, overscan });
    this.items = [];
    this.sel = -1;
    this.rows = new Map();
    this.id = uid('vl');
    this.raf = 0;
    this.spacer = h('div', { class: 'vlist-spacer' });
    this.el = h('div', { class: 'vlist', tabindex: 0, role: 'listbox', 'aria-label': label }, this.spacer);
    this.el.addEventListener('scroll', () => this.schedule(), { passive: true });
    this.el.addEventListener('keydown', (e) => this.key(e));
    this.el.addEventListener('click', (e) => {
      const row = e.target.closest('.vrow');
      if (row) this.select(Number(row.dataset.i), { scroll: false });
    });
    this.el.addEventListener('dblclick', (e) => {
      const row = e.target.closest('.vrow');
      if (row && this.onOpen) this.onOpen(this.items[Number(row.dataset.i)], Number(row.dataset.i));
    });
    new ResizeObserver(() => this.schedule()).observe(this.el);
  }

  setItems(items) {
    this.items = items;
    if (this.sel >= items.length) this.sel = -1;
    this.redraw();
  }

  append(more) {
    for (const it of more) this.items.push(it);
    this.spacer.style.height = `${this.items.length * this.rowHeight}px`;
    this.schedule();
  }

  // replace re-renders one row after its item changed.
  replace(i, item) {
    this.items[i] = item;
    const old = this.rows.get(i);
    if (old) {
      const el = this.row(i);
      old.replaceWith(el);
      this.rows.set(i, el);
    }
  }

  removeAt(i) {
    this.items.splice(i, 1);
    if (this.sel > i) this.sel--;
    else if (this.sel === i) this.sel = Math.min(i, this.items.length - 1);
    this.redraw();
  }

  redraw() {
    for (const el of this.rows.values()) el.remove();
    this.rows.clear();
    this.spacer.style.height = `${this.items.length * this.rowHeight}px`;
    this.schedule();
  }

  schedule() {
    if (!this.raf) this.raf = requestAnimationFrame(() => { this.raf = 0; this.paint(); });
  }

  paint() {
    const top = this.el.scrollTop;
    const height = this.el.clientHeight || 600;
    const first = Math.max(0, Math.floor(top / this.rowHeight) - this.overscan);
    const last = Math.min(this.items.length - 1, Math.ceil((top + height) / this.rowHeight) + this.overscan);
    for (const [i, el] of this.rows) {
      if (i < first || i > last) {
        el.remove();
        this.rows.delete(i);
      }
    }
    for (let i = first; i <= last; i++) {
      if (!this.rows.has(i)) {
        const el = this.row(i);
        this.rows.set(i, el);
        this.spacer.append(el);
      }
    }
    if (this.onEnd && this.items.length && last >= this.items.length - 1 - this.overscan * 2) this.onEnd();
  }

  row(i) {
    const el = h('div', {
      class: 'vrow', role: 'option', id: `${this.id}-${i}`,
      'aria-selected': i === this.sel ? 'true' : 'false', dataset: { i: String(i) },
    });
    el.style.top = `${i * this.rowHeight}px`;
    el.style.height = `${this.rowHeight}px`;
    append(el, [this.render(this.items[i], i)]);
    return el;
  }

  select(i, { scroll = true, notify = true } = {}) {
    if (i < 0 || i >= this.items.length) return;
    this.rows.get(this.sel)?.setAttribute('aria-selected', 'false');
    this.sel = i;
    this.rows.get(i)?.setAttribute('aria-selected', 'true');
    this.el.setAttribute('aria-activedescendant', `${this.id}-${i}`);
    if (scroll) this.scrollTo(i);
    if (notify && this.onSelect) this.onSelect(this.items[i], i);
  }

  scrollTo(i) {
    const y = i * this.rowHeight;
    const view = this.el.clientHeight;
    if (y < this.el.scrollTop) this.el.scrollTop = y;
    else if (y + this.rowHeight > this.el.scrollTop + view) this.el.scrollTop = y + this.rowHeight - view;
  }

  key(e) {
    const page = Math.max(1, Math.floor(this.el.clientHeight / this.rowHeight) - 1);
    const n = this.items.length;
    let next = null;
    switch (e.key) {
      case 'ArrowDown': next = Math.min(n - 1, this.sel + 1); break;
      case 'ArrowUp': next = Math.max(0, this.sel - 1); break;
      case 'PageDown': next = Math.min(n - 1, this.sel + page); break;
      case 'PageUp': next = Math.max(0, this.sel - page); break;
      case 'Home': next = 0; break;
      case 'End': next = n - 1; break;
      case 'Enter':
        if (this.sel >= 0 && this.onOpen) this.onOpen(this.items[this.sel], this.sel);
        e.preventDefault();
        return;
      default:
        return;
    }
    e.preventDefault();
    if (n) this.select(next);
  }

  selected() {
    return this.sel >= 0 ? this.items[this.sel] : undefined;
  }
}

/* ---------------- JSON tree ---------------- */

const CHILD_PAGE = 200;

// jsonTree renders canonical Extended JSON (or plain JSON) as a tree whose
// branches open on demand. depth branches start open.
export function jsonTree(value, { depth = 1 } = {}) {
  const root = h('div', { class: 'jtree' });
  root.append(jnode(null, value, depth));
  return root;
}

function keyEl(key) {
  if (key == null) return null;
  return h('span', { class: 'jk' }, typeof key === 'number' ? String(key) : JSON.stringify(key), h('span', { class: 'jsum' }, ':'));
}

export function scalarEl(v, kind = ejsonKind(v)) {
  switch (kind) {
    case 'string': return h('span', { class: 'js' }, JSON.stringify(v));
    case 'number': case 'int': case 'double': return h('span', { class: 'jn' }, ejsonLabel(v));
    case 'long': return h('span', { class: 'jn' }, `Long("${v.$numberLong}")`);
    case 'decimal': return h('span', { class: 'jn' }, `Decimal128("${v.$numberDecimal}")`);
    case 'bool': return h('span', { class: 'jb' }, String(v));
    case 'null': return h('span', { class: 'jnull' }, 'null');
    case 'date': return h('span', { class: 'jx' }, `ISODate("${ejsonLabel(v)}")`);
    default: return h('span', { class: 'jx' }, ejsonLabel(v));
  }
}

function jnode(key, v, depth) {
  const kind = ejsonKind(v);
  if (kind !== 'object' && kind !== 'array') {
    return h('div', { class: 'jline' }, keyEl(key), scalarEl(v, kind));
  }
  const list = kind === 'array' ? v.map((x, i) => [i, x]) : Object.entries(v);
  const [open, close] = kind === 'array' ? ['[', ']'] : ['{', '}'];
  const count = kind === 'array' ? plural(list.length, 'item') : plural(list.length, 'field');
  if (!list.length) return h('div', { class: 'jline' }, keyEl(key), h('span', { class: 'jsum' }, open + close));
  const wrap = h('div');
  const kids = h('div', { class: 'jnode' });
  const tail = h('div', { class: 'jline' }, h('span', { class: 'jsum' }, close));
  const summary = h('span', { class: 'jsum' }, `${open} ${count} ${close}`);
  const brace = h('span', { class: 'jsum' }, open);
  const toggle = h('button', { type: 'button', class: 'jtoggle', 'aria-expanded': 'false', 'aria-label': `Expand ${count}` }, icon('chevron'));
  let rendered = 0;
  const more = () => {
    const stop = Math.min(list.length, rendered + CHILD_PAGE);
    for (; rendered < stop; rendered++) kids.append(jnode(list[rendered][0], list[rendered][1], depth - 1));
    kids.querySelector(':scope > .jmore-line')?.remove();
    if (rendered < list.length) {
      kids.append(h('div', { class: 'jline jmore-line' }, h('button', { type: 'button', class: 'jmore', onClick: more },
        `show ${fmtInt(Math.min(CHILD_PAGE, list.length - rendered))} more of ${fmtInt(list.length - rendered)}`)));
    }
  };
  const set = (openNow) => {
    toggle.setAttribute('aria-expanded', String(openNow));
    toggle.setAttribute('aria-label', `${openNow ? 'Collapse' : 'Expand'} ${count}`);
    if (openNow && !rendered) more();
    kids.hidden = !openNow;
    tail.hidden = !openNow;
    summary.hidden = openNow;
    brace.hidden = !openNow;
  };
  toggle.addEventListener('click', () => set(toggle.getAttribute('aria-expanded') !== 'true'));
  wrap.append(h('div', { class: 'jline' }, toggle, keyEl(key), brace, summary), kids, tail);
  set(depth > 0);
  return wrap;
}

/* ---------------- charts ---------------- */

// Sparkline draws a series scaled to its maximum; set() redraws it.
export class Sparkline {
  constructor(label) {
    this.w = 240;
    this.hgt = 44;
    this.el = svgEl(`<svg class="spark" viewBox="0 0 ${this.w} ${this.hgt}" preserveAspectRatio="none" role="img"><path class="area"/><path class="line"/></svg>`);
    this.el.setAttribute('aria-label', label);
    [this.area, this.line] = this.el.querySelectorAll('path');
  }

  set(values, capacity = values.length) {
    const n = Math.max(capacity, 2);
    const top = Math.max(1, ...values);
    const start = n - values.length;
    const pts = values.map((v, i) => [((start + i) / (n - 1)) * this.w, this.hgt - 2 - (v / top) * (this.hgt - 8)]);
    if (!pts.length) {
      this.line.removeAttribute('d');
      this.area.removeAttribute('d');
      return;
    }
    const d = pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join('');
    this.line.setAttribute('d', d);
    this.area.setAttribute('d', `${d}L${pts[pts.length - 1][0].toFixed(1)} ${this.hgt}L${pts[0][0].toFixed(1)} ${this.hgt}Z`);
  }
}

// barChart is a horizontal bar per row, labelled and valued in text.
export function barChart(rows, { format = fmtNum, label = 'Bar chart' } = {}) {
  const vals = rows.map((r) => r.value).filter(Number.isFinite);
  const max = Math.max(0, ...vals);
  const el = h('div', { class: 'bars', role: 'list', 'aria-label': label });
  for (const r of rows) {
    const fill = h('i', { class: 'fill' });
    fill.style.width = max > 0 && r.value > 0 ? `${(r.value / max) * 100}%` : '0';
    el.append(h('div', { class: 'bars-row', role: 'listitem' },
      h('span', { class: 'lbl', title: r.label }, r.label),
      h('span', { class: 'track' }, fill),
      h('span', { class: 'val' }, Number.isFinite(r.value) ? format(r.value) : '–')));
  }
  return el;
}

/* ---------------- EXPLAIN ---------------- */

// planView draws {plan, index, bounds, stages, estimatedRows}, the map
// DOC.FIND ... EXPLAIN and NIL.EXPLAIN reply.
export function planView(p) {
  const arrow = () => h('div', { class: 'plan-arrow', 'aria-hidden': 'true' }, icon('arrow'));
  const src = h('div', { class: `plan-step src${p.plan === 'rowscan' ? ' scan' : ''}` },
    h('span', { class: 'k' }, 'source'),
    h('span', { class: 'v' }, p.plan === 'index' ? `index ${p.index ?? ''}` : String(p.plan)),
    (p.bounds || []).map((b) => h('span', { class: 'd' }, b)),
    h('span', { class: 'd' }, `≈ ${fmtInt(p.estimatedRows)} ${Number(p.estimatedRows) === 1 ? 'row' : 'rows'} read`));
  const el = h('div', { class: 'plan', role: 'list', 'aria-label': 'Query plan' }, src);
  for (const s of p.stages || []) {
    el.append(arrow(), h('div', { class: 'plan-step', role: 'listitem' }, h('span', { class: 'k' }, 'stage'), h('span', { class: 'v' }, s)));
  }
  el.append(arrow(), h('div', { class: 'plan-step' }, h('span', { class: 'k' }, 'result'), h('span', { class: 'v' }, 'documents')));
  return el;
}

/* ---------------- tables ---------------- */

// table builds a simple table. cols: [{label, num, mono, cls}]; rows are
// arrays of cells (strings or nodes).
export function table(cols, rows, { empty = 'Nothing here yet', onRow, rowClass, fixed } = {}) {
  const t = h('table', { class: `table${onRow ? ' clickable' : ''}${fixed ? ' fixed' : ''}` },
    h('thead', null, h('tr', null, cols.map((c) => h('th', { class: c.num ? 'num' : c.cls || '', scope: 'col' }, c.label)))));
  const body = h('tbody');
  if (!rows.length) {
    body.append(h('tr', null, h('td', { colSpan: cols.length, class: 'faint' }, empty)));
  }
  rows.forEach((r, i) => {
    const tr = h('tr', { class: rowClass ? rowClass(i) : '' },
      r.map((cell, j) => h('td', { class: [cols[j]?.num ? 'num' : '', cols[j]?.mono ? 'mono' : '', cols[j]?.cls || ''].join(' ').trim() }, cell)));
    if (onRow) tr.addEventListener('click', (e) => { if (!e.target.closest('button')) onRow(i, tr); });
    body.append(tr);
  });
  t.append(body);
  return t;
}

export { clear };
