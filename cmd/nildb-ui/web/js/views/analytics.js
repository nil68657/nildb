// Analytics: NIL.AGGREGATE, NIL.COUNT and NIL.DISTINCT on a snapshot lease
// (a fresh one per run, or one pinned with ROCKS.SNAPSHOT CREATE and passed
// as AT <id>), NIL.EXPLAIN plans, results as a chart and a table, and
// NIL.KEYSTATS for the Redis side.

import {
  h, clear, api, text, num, items, field, isErr, toJS, fmtInt, fmtNum, fmtDuration, fmtMs, iconButton, button,
  ejsonKind, ejsonLabel, ejsonNumber, parseJSONArg, ReplyError,
} from '../lib.js';
import {
  toast, toastError, confirmDialog, formDialog, seg, spinner, emptyState, jsonTree, planView, table, barChart,
} from '../ui.js';

const ROW_CAP = 10000;
const CHART_ROWS = 40;

function card(title, ...extra) {
  const body = h('div', { class: 'card-body' });
  const el = h('section', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, title), ...extra), body);
  return { el, body };
}

// sampleFields reads one document and returns its string and numeric
// top-level fields, for the pipeline templates.
function sampleFields(doc) {
  const str = [];
  const numf = [];
  for (const [k, v] of Object.entries(doc || {})) {
    if (k === '_id') continue;
    const kind = ejsonKind(v);
    if (kind === 'string') str.push(k);
    if (['int', 'long', 'double', 'decimal'].includes(kind)) numf.push(k);
  }
  return { str, num: numf };
}

function templates(f) {
  const g = f.str[0] || 'city';
  const n = f.num[0] || 'price';
  const lab = f.str.find((x) => x !== g) || g;
  return [
    [`Count by ${g}`, [{ $group: { _id: `$${g}`, n: { $sum: 1 } } }, { $sort: { n: -1 } }, { $limit: 20 }]],
    [`Average ${n} by ${g}`, [{ $group: { _id: `$${g}`, avg: { $avg: `$${n}` }, n: { $sum: 1 } } }, { $sort: { avg: -1 } }]],
    [`Top 10 by ${n}`, [{ $sort: { [n]: -1 } }, { $limit: 10 }, { $project: { _id: 0, [lab]: 1, [n]: 1 } }]],
    [`Totals of ${n}`, [{ $group: { _id: null, docs: { $sum: 1 }, total: { $sum: `$${n}` }, min: { $min: `$${n}` }, max: { $max: `$${n}` } } }]],
  ];
}

const pretty = (pipeline) => '[\n' + pipeline.map((s) => '  ' + JSON.stringify(s)).join(',\n') + '\n]';

export function create(app) {
  const collSel = h('select', { class: 'select', 'aria-label': 'Collection' });
  let mode = 'aggregate';
  const modeSeg = seg([['aggregate', 'Aggregate'], ['count', 'Count'], ['distinct', 'Distinct']], mode, (m) => setMode(m), 'Command');
  const tplSel = h('select', { class: 'select', 'aria-label': 'Pipeline template' });
  const tplBox = h('label', { class: 'field' }, h('span', null, 'Template'), tplSel);
  const pipeIn = h('textarea', { class: 'textarea mono', rows: 9, spellcheck: false, 'aria-label': 'Pipeline, a JSON array of stages' });
  const filterIn = h('textarea', { class: 'textarea mono', rows: 3, spellcheck: false, 'aria-label': 'Filter', placeholder: '{"city": "Paris"}' }, '{}');
  const distinctIn = h('input', { class: 'input mono', 'aria-label': 'Field', placeholder: 'city', spellcheck: false });
  const timeoutIn = h('input', { class: 'input mono', type: 'number', min: '1', step: '1', placeholder: 'none', 'aria-label': 'Timeout in milliseconds' });
  const leaseSel = h('select', { class: 'select', 'aria-label': 'Snapshot' });
  const runBtn = button('Run', () => run(false), { cls: 'primary', iconName: 'play', title: 'Run (Ctrl Enter)' });
  const explainBtn = button('Explain', () => run(true), { title: 'NIL.EXPLAIN: the plan, without running it' });
  const qErr = h('div', { class: 'notice err', hidden: true, role: 'alert' });

  const pipeBox = h('label', { class: 'field' }, h('span', null, 'Pipeline'), pipeIn);
  const filterBox = h('label', { class: 'field', hidden: true }, h('span', null, 'Filter'), filterIn);
  const distinctBox = h('label', { class: 'field', hidden: true }, h('span', null, 'Distinct field'), distinctIn);

  const cQuery = card('Query', h('span', { class: 'faint' }, 'reads a lease, skips the block cache'));
  clear(cQuery.body, h('div', { class: 'stack' },
    h('div', { class: 'row wrap' }, h('label', { class: 'field grow' }, h('span', null, 'Collection'), collSel), tplBox),
    h('div', { class: 'row wrap' }, modeSeg),
    pipeBox, distinctBox, filterBox,
    h('div', { class: 'row wrap' },
      h('label', { class: 'field' }, h('span', null, 'Snapshot'), leaseSel),
      h('label', { class: 'field' }, h('span', null, 'Timeout, ms'), timeoutIn),
      h('div', { class: 'grow' }), explainBtn, runBtn),
    qErr));

  const leaseList = h('div');
  const cLeases = card('Snapshot leases',
    iconButton('refresh', 'Reload leases', () => loadLeases()),
    button('Create lease', () => createLease(), { cls: 'sm primary', iconName: 'lease' }));
  cLeases.body.append(h('p', { class: 'faint' }, 'Pin a lease to read one sequence number across runs: writes made after it stay invisible to AT <id> until it expires or you release it.'), leaseList);

  const resultInfo = h('span', { class: 'faint tnum' });
  let resultMode = 'chart';
  const resultSeg = seg([['chart', 'Chart'], ['table', 'Table'], ['json', 'JSON']], resultMode, (m) => { resultMode = m; paintResult(); }, 'Result view');
  const cResult = card('Result', resultInfo, resultSeg);
  cResult.body.classList.add('flush');

  const ksDB = h('select', { class: 'select', 'aria-label': 'Database for NIL.KEYSTATS' },
    h('option', { value: 'current' }, 'current db'), h('option', { value: 'all' }, 'all databases'));
  const cKeys = card('Redis keys by type', ksDB, button('Run NIL.KEYSTATS', () => keyStats(), { cls: 'sm' }));
  cKeys.body.append(h('p', { class: 'faint' }, 'Counts every key by type on a fresh lease, reading only the meta column family.'));

  const el = h('section', { class: 'view view-scroll', 'aria-label': 'Analytics' },
    h('div', { class: 'view-head' }, h('h1', null, 'Analytics'), h('span', { class: 'sub' }, 'NIL.AGGREGATE, NIL.COUNT and NIL.DISTINCT on snapshot leases')),
    h('div', { class: 'view-body' },
      h('div', { class: 'grid cols-2' }, cQuery.el, h('div', { class: 'grid' }, cLeases.el, cKeys.el)),
      cResult.el));

  let leases = [];
  let clockOffset = 0;
  let rows = [];
  let lastPlan = null;
  let labelKey = '';
  let valueKey = '';

  function setMode(m) {
    mode = m;
    modeSeg.set(m);
    pipeBox.hidden = m !== 'aggregate';
    tplBox.hidden = m !== 'aggregate';
    filterBox.hidden = m === 'aggregate';
    distinctBox.hidden = m !== 'distinct';
    explainBtn.hidden = m !== 'aggregate';
  }

  async function loadCollections() {
    try {
      const names = items(await api.ok(['DOC.COLLECTIONS'])).map(text);
      const cur = collSel.value;
      clear(collSel, names.length ? names.map((n) => h('option', { value: n }, n)) : h('option', { value: '' }, 'no collections'));
      if (names.includes(cur)) collSel.value = cur;
      await loadTemplates();
    } catch (e) {
      toastError(e, 'Could not list collections');
    }
  }

  async function loadTemplates() {
    const ns = collSel.value;
    let f = { str: [], num: [] };
    if (ns) {
      try {
        const d = await api.run(['DOC.FINDONE', ns, '{}']);
        if (d.t === 'bulk') f = sampleFields(JSON.parse(text(d)));
      } catch { /* the templates fall back to generic field names */ }
    }
    const tpls = templates(f);
    clear(tplSel, tpls.map(([label], i) => h('option', { value: String(i) }, label)));
    tplSel.onchange = () => {
      pipeIn.value = pretty(tpls[Number(tplSel.value)][1]);
      pipeIn.dataset.fromTemplate = 'yes';
    };
    if (!pipeIn.value.trim() || pipeIn.dataset.fromTemplate === 'yes') {
      pipeIn.value = pretty(tpls[0][1]);
      pipeIn.dataset.fromTemplate = 'yes';
    }
    if (!distinctIn.value && f.str[0]) distinctIn.value = f.str[0];
  }

  pipeIn.addEventListener('input', () => { pipeIn.dataset.fromTemplate = 'no'; });
  collSel.addEventListener('change', () => {
    pipeIn.dataset.fromTemplate = 'yes';
    loadTemplates();
  });

  async function loadLeases() {
    try {
      const [list, time] = await api.batch([['ROCKS.SNAPSHOT', 'LIST'], ['TIME']], { db: 0 });
      if (isErr(list)) throw new ReplyError(list.v);
      if (!isErr(time)) {
        const [s, us] = items(time).map((x) => Number(text(x)));
        clockOffset = s * 1000 + us / 1000 - Date.now();
      }
      leases = toJS(list);
      paintLeases();
    } catch (e) {
      clear(leaseList, h('span', { class: 'r-err' }, e.message));
    }
  }

  function paintLeases() {
    const now = Date.now() + clockOffset;
    const live = leases.filter((l) => l.expires_ms > now);
    const cur = leaseSel.value;
    clear(leaseSel, h('option', { value: '' }, 'fresh lease per run'),
      live.map((l) => h('option', { value: String(l.id) }, `AT ${l.id} · seq ${fmtInt(l.seq)} · ${fmtDuration(l.expires_ms - now)} left`)));
    if (live.some((l) => String(l.id) === cur)) leaseSel.value = cur;
    clear(leaseList, table(
      [{ label: 'Id' }, { label: 'Seq', num: true }, { label: 'Owner' }, { label: 'Expires in', num: true }, { label: '', cls: 'actions' }],
      live.map((l) => [
        h('span', { class: 'mono' }, String(l.id)), fmtInt(l.seq), h('span', { class: 'mono' }, l.owner), fmtDuration(l.expires_ms - now),
        h('span', { class: 'row' },
          button('Use', () => { leaseSel.value = String(l.id); }, { cls: 'sm ghost' }),
          iconButton('trash', `Release lease ${l.id}`, () => release(l), 'sm')),
      ]),
      { empty: 'No open leases' }));
  }

  async function createLease() {
    const r = await formDialog({
      title: 'Create a snapshot lease',
      intro: 'ROCKS.SNAPSHOT CREATE pins the current sequence number. NilDB caps the TTL at nildb.lease-max, 600 seconds by default.',
      fields: [{ name: 'ttl', label: 'TTL, seconds', type: 'number', min: 1, step: 1, value: '300', autofocus: true }],
      submit: 'Create',
      onSubmit: async (v) => {
        if (!/^\d+$/.test(v.ttl) || Number(v.ttl) < 1) throw new Error('TTL must be a whole number of seconds');
        return toJS(await api.ok(['ROCKS.SNAPSHOT', 'CREATE', 'TTL', v.ttl], { db: 0 }));
      },
    });
    if (!r) return;
    toast(`Lease ${r.id} pins sequence ${fmtInt(r.seq)}`, 'ok');
    await loadLeases();
    leaseSel.value = String(r.id);
  }

  async function release(l) {
    const ok = await confirmDialog({ title: 'Release this lease?', message: 'Runs that pass AT with it fail from now on.', name: `lease ${l.id} · seq ${fmtInt(l.seq)}`, confirm: 'Release' });
    if (!ok) return;
    try {
      await api.ok(['ROCKS.SNAPSHOT', 'RELEASE', String(l.id)], { db: 0 });
      loadLeases();
    } catch (e) {
      toastError(e, 'Release failed');
    }
  }

  function commonOpts(args) {
    if (leaseSel.value) args.push('AT', leaseSel.value);
    const t = timeoutIn.value.trim();
    if (t) {
      if (!/^\d+$/.test(t) || Number(t) < 1) throw new Error('Timeout must be a whole number of milliseconds');
      args.push('TIMEOUT', t);
    }
    return args;
  }

  function showErr(msg) {
    qErr.textContent = msg;
    qErr.hidden = !msg;
  }

  const leaseNote = () => (leaseSel.value ? `AT ${leaseSel.value}` : 'fresh lease');
  const cursorId = (c) => String(field(c, 'id')?.v ?? '0');

  async function run(explain) {
    const ns = collSel.value;
    if (!ns) {
      showErr('Pick a collection');
      return;
    }
    showErr('');
    runBtn.disabled = true;
    explainBtn.disabled = true;
    const t0 = performance.now();
    try {
      if (mode === 'aggregate') {
        const pipeline = parseJSONArg(pipeIn.value, 'The pipeline', { array: true });
        if (explain) {
          lastPlan = toJS(await api.ok(['NIL.EXPLAIN', ns, pipeline], { db: 0 }));
          resultMode = 'plan';
          resultInfo.textContent = `NIL.EXPLAIN ${ns}`;
          paintResult();
          return;
        }
        let c = field(await api.ok(commonOpts(['NIL.AGGREGATE', ns, pipeline, 'BATCH', '1000']), { db: 0 }), 'cursor');
        const out = items(field(c, 'firstBatch')).map((v) => JSON.parse(text(v)));
        let reads = 1;
        while (cursorId(c) !== '0' && out.length < ROW_CAP) {
          c = field(await api.ok(['DOC.CURSOR', 'READ', cursorId(c), 'COUNT', '1000'], { db: 0 }), 'cursor');
          out.push(...items(field(c, 'nextBatch')).map((v) => JSON.parse(text(v))));
          reads++;
        }
        const left = cursorId(c);
        if (left !== '0') await api.run(['DOC.CURSOR', 'DEL', left], { db: 0 });
        rows = out;
        if (resultMode === 'plan') resultMode = 'chart';
        resultInfo.textContent = `${fmtInt(rows.length)} rows${left !== '0' ? ` (the first ${fmtInt(ROW_CAP)})` : ''} · ${reads} ${reads === 1 ? 'batch' : 'batches'} · ${fmtMs(performance.now() - t0)} · ${leaseNote()}`;
      } else if (mode === 'count') {
        const filter = parseJSONArg(filterIn.value, 'The filter');
        const n = num(await api.ok(commonOpts(['NIL.COUNT', ns, filter]), { db: 0 }));
        rows = [{ _id: 'matching documents', count: { $numberLong: String(n) } }];
        if (resultMode === 'plan') resultMode = 'chart';
        resultInfo.textContent = `NIL.COUNT ${fmtInt(n)} · ${fmtMs(performance.now() - t0)} · ${leaseNote()}`;
      } else {
        const f = distinctIn.value.trim();
        if (!f) throw new Error('Name the field');
        const filter = parseJSONArg(filterIn.value, 'The filter');
        const vals = items(await api.ok(commonOpts(['NIL.DISTINCT', ns, f, filter]), { db: 0 })).map((v) => JSON.parse(text(v)));
        rows = vals.map((v) => ({ [f]: v }));
        if (resultMode === 'chart' || resultMode === 'plan') resultMode = 'table';
        resultInfo.textContent = `NIL.DISTINCT ${fmtInt(vals.length)} values of ${f} · ${fmtMs(performance.now() - t0)} · ${leaseNote()}`;
      }
      resultSeg.set(resultMode);
      pickAxes();
      paintResult();
      loadLeases();
    } catch (e) {
      showErr(e.message);
    } finally {
      runBtn.disabled = false;
      explainBtn.disabled = false;
    }
  }

  // pickAxes chooses the chart's label field (_id or the first text field)
  // and value field (the first numeric field).
  function pickAxes() {
    const keys = [...new Set(rows.slice(0, 50).flatMap((r) => Object.keys(r)))];
    const numeric = keys.filter((k) => rows.some((r) => Number.isFinite(ejsonNumber(r[k]))));
    if (!keys.includes(labelKey)) labelKey = keys.includes('_id') ? '_id' : keys.find((k) => !numeric.includes(k)) || keys[0] || '';
    if (!numeric.includes(valueKey) || valueKey === labelKey) valueKey = numeric.find((k) => k !== labelKey) || '';
  }

  function paintResult() {
    const body = cResult.body;
    if (resultMode === 'plan') {
      clear(body, lastPlan ? planView(lastPlan) : emptyState('analytics', 'No plan yet', ''));
      return;
    }
    if (!rows.length) {
      clear(body, emptyState('analytics', 'No rows', 'Run a pipeline to see its result here.'));
      return;
    }
    if (resultMode === 'json') {
      clear(body, jsonTree(rows, { depth: 2 }));
      return;
    }
    const keys = [...new Set(rows.slice(0, 200).flatMap((r) => Object.keys(r)))].slice(0, 20);
    if (resultMode === 'table') {
      clear(body, h('div', { class: 'table-wrap' }, table(
        keys.map((k) => ({ label: k, cls: 'mono' })),
        rows.slice(0, 1000).map((r) => keys.map((k) => (k in r ? h('span', { class: 'clip mono' }, ejsonLabel(r[k])) : ''))))));
      return;
    }
    const numeric = keys.filter((k) => rows.some((r) => Number.isFinite(ejsonNumber(r[k]))));
    if (!numeric.length || !valueKey) {
      clear(body, emptyState('analytics', 'Nothing numeric to chart', 'The rows have no numeric field; the table shows them.'));
      return;
    }
    const labelSel = h('select', { class: 'select', 'aria-label': 'Label field' }, keys.map((k) => h('option', { value: k, selected: k === labelKey }, k)));
    const valueSel = h('select', { class: 'select', 'aria-label': 'Value field' }, numeric.map((k) => h('option', { value: k, selected: k === valueKey }, k)));
    labelSel.onchange = () => { labelKey = labelSel.value; paintResult(); };
    valueSel.onchange = () => { valueKey = valueSel.value; paintResult(); };
    const data = rows.slice(0, CHART_ROWS).map((r) => ({ label: ejsonLabel(r[labelKey] ?? null), value: ejsonNumber(r[valueKey]) }));
    clear(body,
      h('div', { class: 'detail-tools' }, h('span', { class: 'faint' }, 'label'), labelSel, h('span', { class: 'faint' }, 'value'), valueSel,
        rows.length > CHART_ROWS ? h('span', { class: 'faint' }, `the first ${CHART_ROWS} of ${fmtInt(rows.length)} rows`) : null),
      barChart(data, { label: `${valueKey} by ${labelKey}`, format: (x) => fmtNum(x) }));
  }

  async function keyStats() {
    const args = ['NIL.KEYSTATS'];
    if (ksDB.value === 'current') args.push(String(app.db));
    clear(cKeys.body, h('div', { class: 'row' }, spinner(), 'Counting keys'));
    try {
      const t0 = performance.now();
      const s = toJS(await api.ok(args, { db: 0 }));
      const types = ['string', 'hash', 'list', 'set', 'zset'];
      clear(cKeys.body,
        h('p', { class: 'faint' }, `db ${s.db}: ${fmtInt(s.keys)} keys, ${fmtInt(s.expires)} with a TTL · ${fmtMs(performance.now() - t0)}`),
        barChart(types.map((t) => ({ label: t, value: Number(s[t] || 0) })), { label: 'Keys by type', format: fmtInt }));
    } catch (e) {
      clear(cKeys.body, h('span', { class: 'r-err' }, e.message));
    }
  }

  for (const area of [pipeIn, filterIn]) {
    area.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(false); }
    });
  }

  let first = true;
  let leaseTimer = 0;
  setMode('aggregate');
  paintResult();
  return {
    el,
    async show() {
      clearInterval(leaseTimer);
      leaseTimer = setInterval(() => { if (!document.hidden) paintLeases(); }, 5000);
      if (first) {
        first = false;
        await loadCollections();
        await loadLeases();
        if (collSel.value) run(false);
      }
    },
    hide() { clearInterval(leaseTimer); },
    focusSearch() { pipeIn.focus(); },
    async createLease() {
      if (first) {
        first = false;
        await loadCollections();
        await loadLeases();
      }
      createLease();
    },
  };
}
