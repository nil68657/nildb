// Documents: DOC.* collections, find with filter, projection, sort, skip
// and limit, results as a table or JSON tree with cursor paging, EXPLAIN,
// index create and drop, and document insert, edit and delete.

import {
  h, clear, api, text, num, items, field, isErr, toJS, fmtInt, fmtBytes, iconButton, button,
  ejsonLabel, ejsonEdit, ejsonKind, parseJSONArg, copyText, plural, ReplyError,
} from '../lib.js';
import {
  toast, toastError, confirmDialog, formDialog, seg, spinner, emptyState, jsonTree, planView, table, scalarEl,
} from '../ui.js';

const BATCH = 50;
const MAX_COLS = 24;

function cursorOf(reply, batchName) {
  const c = field(reply, 'cursor');
  return { id: field(c, 'id'), docs: items(field(c, batchName)).map((v) => JSON.parse(text(v))) };
}

function cellFor(v) {
  const kind = ejsonKind(v);
  if (kind === 'object' || kind === 'array') return h('span', { class: 'faint mono clip', title: ejsonLabel(v) }, ejsonLabel(v));
  const node = kind === 'string' ? h('span', null, v) : scalarEl(v, kind);
  node.classList.add('clip');
  node.title = ejsonLabel(v);
  return node;
}

export function create(app) {
  const ro = () => app.readonly;
  const collList = h('div', { class: 'pane-body', role: 'list', 'aria-label': 'Collections' });
  const collFoot = h('div', { class: 'pane-foot' });
  const left = h('div', { class: 'pane' },
    h('div', { class: 'pane-head' }, h('h2', { class: 'grow' }, 'Collections'),
      iconButton('refresh', 'Reload collections', () => loadCollections()),
      ro() ? null : button('New', () => newCollection(), { cls: 'sm primary', iconName: 'plus' })),
    collList, collFoot);
  const right = h('div', { class: 'pane' });
  const el = h('section', { class: 'view', 'aria-label': 'Documents' },
    h('div', { class: 'view-head' }, h('h1', null, 'Documents'), h('span', { class: 'sub' }, 'DOC.* collections, queried with MongoDB filters in Extended JSON')),
    h('div', { class: 'split docs' }, left, right));

  let colls = [];
  let ns = '';
  const lastQuery = new Map();

  async function loadCollections() {
    try {
      const names = items(await api.ok(['DOC.COLLECTIONS'])).map(text);
      const stats = names.length && names.length <= 300 ? await api.batch(names.map((n) => ['DOC.STATS', n])) : [];
      colls = names.map((n, i) => ({ ns: n, count: stats[i] && !isErr(stats[i]) ? num(field(stats[i], 'count')) : null }));
      paintCollections();
    } catch (e) {
      clear(collList, h('div', { class: 'empty' }, h('span', { class: 'r-err' }, e.message)));
    }
  }

  function paintCollections() {
    const go = (c) => app.go(`docs/${encodeURIComponent(c.ns)}`);
    clear(collList, colls.length ? colls.map((c) => h('div', {
      class: 'coll-item', role: 'listitem', tabindex: 0, 'aria-current': String(c.ns === ns),
      onClick: () => go(c),
      onKeydown: (e) => { if (e.key === 'Enter') go(c); },
    }, h('span', { class: 'name', title: c.ns }, c.ns), h('span', { class: 'count' }, c.count == null ? '' : fmtInt(c.count))))
      : emptyState('docs', 'No collections', ro() ? 'This NilDB has no DOC.* collections.' : 'Create one, or DOC.INSERT creates it on first write.'));
    clear(collFoot, `${fmtInt(colls.length)} ${colls.length === 1 ? 'collection' : 'collections'}`);
  }

  async function newCollection() {
    if (ro()) return;
    const r = await formDialog({
      title: 'New collection',
      intro: 'Namespaces are database.collection, such as shop.orders.',
      fields: [{ name: 'ns', label: 'Namespace', mono: true, autofocus: true, placeholder: 'shop.orders' }],
      submit: 'Create',
      onSubmit: async (v) => {
        if (!v.ns.trim()) throw new Error('Type a namespace');
        await api.ok(['DOC.CREATE', v.ns.trim()]);
        return v.ns.trim();
      },
    });
    if (!r) return;
    await loadCollections();
    app.go(`docs/${encodeURIComponent(r)}`);
  }

  /* ---------------- one collection ---------------- */

  let view = null;

  function openCollection(name) {
    ns = name;
    paintCollections();
    if (view) view.dispose();
    view = collectionView(name);
    clear(right, view.el);
  }

  function collectionView(name) {
    const q = lastQuery.get(name) || { filter: '{}', project: '', sort: '', skip: '', limit: '' };
    const statsEl = h('div', { class: 'detail-meta' });
    const inputs = {
      filter: h('input', { class: 'input mono', value: q.filter, 'aria-label': 'Filter', placeholder: '{"city": "Paris"}', spellcheck: false }),
      project: h('input', { class: 'input mono', value: q.project, 'aria-label': 'Projection', placeholder: '{"name": 1}', spellcheck: false }),
      sort: h('input', { class: 'input mono', value: q.sort, 'aria-label': 'Sort', placeholder: '{"rating": -1}', spellcheck: false }),
      skip: h('input', { class: 'input mono', value: q.skip, 'aria-label': 'Skip', placeholder: '0', inputMode: 'numeric' }),
      limit: h('input', { class: 'input mono', value: q.limit, 'aria-label': 'Limit', placeholder: 'none', inputMode: 'numeric' }),
    };
    const fieldBox = (label, input, cls = '') => h('label', { class: `field ${cls}` }, h('span', null, label), input);
    const runBtn = button('Find', () => run(false), { cls: 'primary', iconName: 'play', title: 'Run DOC.FIND (Enter)' });
    const explainBtn = button('Explain', () => run(true), { title: 'Show the plan DOC.FIND ... EXPLAIN picks' });
    const query = h('div', { class: 'query' },
      fieldBox('filter', inputs.filter, 'q-filter'), fieldBox('project', inputs.project), fieldBox('sort', inputs.sort),
      fieldBox('skip', inputs.skip), fieldBox('limit', inputs.limit), h('div', { class: 'row' }, runBtn, explainBtn));
    const qErr = h('div', { class: 'notice err grow', role: 'alert' });
    const errRow = h('div', { class: 'detail-tools', hidden: true }, qErr);
    const planBox = h('div', { class: 'card', hidden: true });
    const resultInfo = h('span', { class: 'faint tnum' });
    const more = button('Load more', () => loadMore(), { cls: 'sm' });
    let mode = 'table';
    const modeSeg = seg([['table', 'Table'], ['json', 'JSON']], mode, (m) => { mode = m; paint(); }, 'Result view');
    const results = h('div', { class: 'pane-body' });
    const drawer = h('div', { class: 'doc-drawer', hidden: true });
    const docsTab = h('div', { class: 'docs-results' },
      query, errRow, planBox,
      h('div', { class: 'detail-tools' }, resultInfo, h('div', { class: 'grow' }), more, modeSeg,
        ro() ? null : button('Insert', () => insertDoc(), { cls: 'sm primary', iconName: 'plus' })),
      results);
    const indexTab = h('div', { class: 'pane-body', hidden: true });
    const tabs = seg([['docs', 'Documents'], ['indexes', 'Indexes']], 'docs', (t) => {
      docsTab.hidden = t !== 'docs';
      indexTab.hidden = t !== 'indexes';
      drawer.hidden = true;
      if (t === 'indexes') loadIndexes();
    }, 'Collection tabs');
    const el = h('div', { class: 'pane' },
      h('div', { class: 'detail-head' },
        h('div', { class: 'detail-title' }, h('div', { class: 'detail-name' }, name), statsEl),
        h('div', { class: 'row wrap' }, tabs,
          iconButton('refresh', 'Reload', () => { loadStats(); run(false); }),
          ro() ? null : button('Drop', () => dropCollection(), { cls: 'sm danger', iconName: 'trash' }))),
      h('div', { class: 'docs-main' }, h('div', { class: 'docs-results' }, docsTab, indexTab), drawer));

    let docs = [];
    let cursorId = null;
    let total = null;
    let disposed = false;
    let selected = -1;

    for (const inp of Object.values(inputs)) {
      inp.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); run(false); }
      });
    }

    const cursorOpen = () => !!cursorId && String(cursorId.v) !== '0';

    async function closeCursor() {
      if (cursorOpen()) {
        const id = String(cursorId.v);
        cursorId = null;
        await api.run(['DOC.CURSOR', 'DEL', id]).catch(() => {});
      }
    }

    async function loadStats() {
      try {
        const s = toJS(await api.ok(['DOC.STATS', name]));
        clear(statsEl,
          h('span', null, plural(s.count, 'document')), h('span', { title: 'DOC.STATS counts SST files, not data still in memtables' }, `${fmtBytes(s.size)} in SST files`),
          h('span', null, `${fmtInt(s.nindexes)} ${s.nindexes === 1 ? 'index' : 'indexes'}`));
      } catch (e) {
        clear(statsEl, h('span', { class: 'r-err' }, e.message));
      }
    }

    function readQuery() {
      const filter = parseJSONArg(inputs.filter.value, 'Filter');
      const project = parseJSONArg(inputs.project.value, 'Projection');
      const sort = parseJSONArg(inputs.sort.value, 'Sort');
      const skip = inputs.skip.value.trim();
      const limit = inputs.limit.value.trim();
      if (skip && !/^\d+$/.test(skip)) throw new Error('Skip must be a whole number');
      if (limit && !/^\d+$/.test(limit)) throw new Error('Limit must be a whole number');
      lastQuery.set(name, { filter: inputs.filter.value, project: inputs.project.value, sort: inputs.sort.value, skip, limit });
      const args = ['DOC.FIND', name, filter];
      if (project !== '{}') args.push('PROJECT', project);
      if (sort !== '{}') args.push('SORT', sort);
      if (skip && skip !== '0') args.push('SKIP', skip);
      if (limit && limit !== '0') args.push('LIMIT', limit);
      args.push('BATCH', String(BATCH));
      return { args, filter };
    }

    function showErr(msg) {
      qErr.textContent = msg;
      errRow.hidden = !msg;
    }

    async function run(explain) {
      let qa;
      try {
        qa = readQuery();
      } catch (e) {
        showErr(e.message);
        return;
      }
      showErr('');
      runBtn.disabled = true;
      explainBtn.disabled = true;
      try {
        if (explain) {
          const plan = await api.ok([...qa.args, 'EXPLAIN']);
          planBox.hidden = false;
          clear(planBox,
            h('div', { class: 'card-head' }, h('h2', null, 'Plan'), h('span', { class: 'faint' }, 'DOC.FIND ... EXPLAIN'),
              iconButton('close', 'Hide the plan', () => { planBox.hidden = true; }, 'sm')),
            planView(toJS(plan)));
          return;
        }
        await closeCursor();
        clear(results, h('div', { class: 'empty' }, spinner(), 'Running DOC.FIND'));
        const [r, cnt] = await api.batch([qa.args, ['DOC.COUNT', name, qa.filter]]);
        if (disposed) return;
        if (isErr(r)) throw new ReplyError(r.v);
        const c = cursorOf(r, 'firstBatch');
        docs = c.docs;
        cursorId = c.id;
        total = isErr(cnt) ? null : num(cnt);
        selected = -1;
        drawer.hidden = true;
        paint();
      } catch (e) {
        showErr(e.message);
        clear(results);
      } finally {
        runBtn.disabled = false;
        explainBtn.disabled = false;
      }
    }

    async function loadMore() {
      if (!cursorOpen()) return;
      more.disabled = true;
      try {
        const r = await api.ok(['DOC.CURSOR', 'READ', String(cursorId.v), 'COUNT', String(BATCH)]);
        const c = cursorOf(r, 'nextBatch');
        docs.push(...c.docs);
        cursorId = c.id;
      } catch (e) {
        toastError(e, 'Could not read the cursor');
        cursorId = null;
      } finally {
        more.disabled = false;
        paint();
      }
    }

    function paint() {
      more.hidden = !cursorOpen();
      resultInfo.textContent = `${fmtInt(docs.length)} shown${total != null ? ` of ${fmtInt(total)} matching` : ''}${cursorOpen() ? ' · cursor open' : ''}`;
      if (!docs.length) {
        clear(results, emptyState('search', 'No documents match', 'Change the filter, or insert a document.'));
        return;
      }
      if (mode === 'json') {
        clear(results, docs.map((d, i) => h('div', { class: 'card-body' },
          h('div', { class: 'row' }, h('span', { class: 'faint tnum' }, `#${i + 1}`), h('div', { class: 'grow' }),
            button('Open', () => openDoc(i), { cls: 'sm ghost' })),
          jsonTree(d, { depth: 1 }))));
        return;
      }
      const cols = ['_id'];
      for (const d of docs) {
        for (const k of Object.keys(d)) if (!cols.includes(k) && cols.length < MAX_COLS) cols.push(k);
      }
      clear(results, h('div', { class: 'table-wrap' }, table(
        cols.map((c) => ({ label: c, cls: 'mono' })),
        docs.map((d) => cols.map((c) => (c in d ? cellFor(d[c]) : ''))),
        { onRow: (i) => openDoc(i), rowClass: (i) => (i === selected ? 'selected' : '') })));
    }

    function openDoc(i) {
      selected = i;
      if (mode === 'table') results.querySelectorAll('tbody tr').forEach((tr, j) => tr.classList.toggle('selected', j === i));
      const d = docs[i];
      drawer.hidden = false;
      const idText = d._id !== undefined ? ejsonLabel(d._id) : `document ${i + 1}`;
      const body = h('div', { class: 'pane-body' }, jsonTree(d, { depth: 2 }));
      const canEdit = !ro() && d._id !== undefined;
      clear(drawer,
        h('div', { class: 'pane-head' }, h('span', { class: 'mono ellipsis grow', title: idText }, idText),
          iconButton('copy', 'Copy as Extended JSON', () => copyText(JSON.stringify(d, null, 2)).then(() => toast('Copied', 'ok'), (e) => toastError(e))),
          canEdit ? button('Edit', () => editDoc(i, body), { cls: 'sm', iconName: 'edit' }) : null,
          canEdit ? button('Delete', () => deleteDoc(i), { cls: 'sm danger', iconName: 'trash' }) : null,
          iconButton('close', 'Close the document', () => { drawer.hidden = true; selected = -1; paint(); }, 'sm')),
        body);
    }

    function editDoc(i, body) {
      const d = docs[i];
      const area = h('textarea', { class: 'textarea editor', spellcheck: false, 'aria-label': 'Document' }, ejsonEdit(d));
      const errBox = h('div', { class: 'field-error', role: 'alert' });
      const save = button('Save', async () => {
        errBox.textContent = '';
        let doc;
        try {
          doc = parseJSONArg(area.value, 'The document');
        } catch (e) {
          errBox.textContent = e.message;
          return;
        }
        try {
          const r = await api.ok(['DOC.REPLACE', name, JSON.stringify({ _id: d._id }), doc]);
          if (num(field(r, 'n')) === 0) throw new Error('No document has this _id any more; it was deleted or its _id changed');
          toast('Saved', 'ok');
          const fresh = await api.ok(['DOC.GET', name, JSON.stringify(d._id)]);
          if (fresh.t !== 'null') docs[i] = JSON.parse(text(fresh));
          paint();
          openDoc(i);
        } catch (e) {
          errBox.textContent = e.message;
        }
      }, { cls: 'sm primary' });
      area.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && (e.key === 's' || e.key === 'Enter')) { e.preventDefault(); save.click(); }
      });
      clear(body,
        h('div', { class: 'detail-tools' }, h('span', { class: 'faint grow' }, 'Relaxed Extended JSON; numbers keep their BSON type'),
          button('Cancel', () => openDoc(i), { cls: 'sm ghost' }), save),
        h('div', { class: 'detail-tools' }, errBox), area);
      area.focus();
    }

    async function deleteDoc(i) {
      const d = docs[i];
      const ok = await confirmDialog({
        title: 'Delete this document?',
        message: `DOC.DELETE removes it from ${name} and from every index. There is no undo.`,
        name: `_id ${ejsonLabel(d._id)}`, confirm: 'Delete document',
      });
      if (!ok) return;
      try {
        await api.ok(['DOC.DELETE', name, JSON.stringify({ _id: d._id })]);
        docs.splice(i, 1);
        if (total != null) total--;
        drawer.hidden = true;
        selected = -1;
        paint();
        loadStats();
        toast('Deleted', 'ok');
      } catch (e) {
        toastError(e, 'Delete failed');
      }
    }

    async function insertDoc() {
      const r = await formDialog({
        title: `Insert into ${name}`,
        wide: true,
        intro: 'One document in Extended JSON. Without an _id NilDB assigns an ObjectId.',
        fields: [{ name: 'doc', label: 'Document', type: 'textarea', rows: 12, value: '{\n  \n}', autofocus: true }],
        submit: 'Insert',
        onSubmit: async (v) => {
          const doc = parseJSONArg(v.doc, 'The document');
          const res = toJS(await api.ok(['DOC.INSERT', name, doc]));
          if (res.writeErrors && res.writeErrors.length) throw new Error(res.writeErrors[0].errmsg);
          const id = res.ids && res.ids[0];
          return id ? ejsonLabel(JSON.parse(id)) : 'the new document';
        },
      });
      if (r == null) return;
      toast(`Inserted ${r}`, 'ok');
      loadStats();
      run(false);
    }

    async function dropCollection() {
      const r = await formDialog({
        title: 'Drop this collection?',
        intro: `DOC.DROP deletes every document and index in ${name}. Type the namespace to confirm.`,
        fields: [{ name: 'ns', label: 'Namespace', mono: true, autofocus: true, placeholder: name }],
        submit: 'Drop collection',
        danger: true,
        onSubmit: async (v) => {
          if (v.ns !== name) throw new Error(`Type ${name} exactly`);
          await api.ok(['DOC.DROP', name]);
        },
      });
      if (!r) return;
      toast(`Dropped ${name}`, 'ok');
      ns = '';
      await loadCollections();
      app.go('docs');
    }

    /* ---------------- indexes ---------------- */

    async function loadIndexes() {
      clear(indexTab, h('div', { class: 'empty' }, spinner(), 'Loading indexes'));
      try {
        const [ix, st] = await api.batch([['DOC.INDEXES', name], ['DOC.STATS', name]]);
        if (isErr(ix)) throw new ReplyError(ix.v);
        const sizes = isErr(st) ? {} : (toJS(st).indexSizes || {});
        const rows = toJS(ix);
        clear(indexTab,
          h('div', { class: 'detail-tools' }, h('span', { class: 'faint grow' }, `${rows.length} ${rows.length === 1 ? 'index' : 'indexes'}; NilDB v1 builds every index synchronously`),
            ro() ? null : button('Create index', () => createIndex(), { cls: 'sm primary', iconName: 'plus' })),
          h('div', { class: 'table-wrap' }, table(
            [{ label: 'Name' }, { label: 'Key' }, { label: 'Kind' }, { label: 'Unique' }, { label: 'Sparse' }, { label: 'Multikey' },
              { label: 'State' }, { label: 'S2 levels' }, { label: 'Size', num: true }, { label: '', cls: 'actions' }],
            rows.map((x) => [
              h('span', { class: 'mono' }, x.name), h('span', { class: 'mono' }, x.key), x.kind,
              x.unique ? 'yes' : 'no', x.sparse ? 'yes' : 'no', x.multikey ? 'yes' : 'no', x.state,
              x.kind === '2dsphere' ? `${x.coarsest} to ${x.finest}, ${x.maxCells} cells` : '',
              sizes[x.name] != null ? fmtBytes(sizes[x.name]) : '',
              ro() || x.name === '_id_' ? '' : iconButton('trash', `Drop index ${x.name}`, () => dropIndex(x.name), 'sm'),
            ]))));
      } catch (e) {
        clear(indexTab, h('div', { class: 'empty' }, h('span', { class: 'r-err' }, e.message)));
      }
    }

    async function createIndex() {
      const r = await formDialog({
        title: `Create an index on ${name}`,
        intro: 'A btree key such as {"city": 1, "rating": -1}, or a 2dsphere key such as {"loc": "2dsphere"}.',
        fields: [
          { name: 'key', label: 'Key', mono: true, autofocus: true, placeholder: '{"city": 1}' },
          { name: 'iname', label: 'Name (optional)', mono: true, placeholder: 'city_1' },
          { name: 'unique', label: 'Unique', type: 'checkbox' },
          { name: 'sparse', label: 'Sparse', type: 'checkbox' },
        ],
        submit: 'Create',
        onSubmit: async (v) => {
          const key = parseJSONArg(v.key, 'The key');
          const args = ['DOC.CREATEINDEX', name, key];
          if (v.iname.trim()) args.push('NAME', v.iname.trim());
          if (v.unique) args.push('UNIQUE');
          if (v.sparse) args.push('SPARSE');
          return text(await api.ok(args));
        },
      });
      if (!r) return;
      toast(`Index ${r} is ready`, 'ok');
      loadIndexes();
      loadStats();
    }

    async function dropIndex(ix) {
      const ok = await confirmDialog({ title: 'Drop this index?', message: `Queries on ${name} that use it fall back to another index or a row scan.`, name: ix, confirm: 'Drop index' });
      if (!ok) return;
      try {
        await api.ok(['DOC.DROPINDEX', name, ix]);
        toast(`Dropped ${ix}`, 'ok');
        loadIndexes();
        loadStats();
      } catch (e) {
        toastError(e, 'Drop failed');
      }
    }

    loadStats();
    run(false);
    return {
      el,
      focus() { inputs.filter.focus(); },
      dispose() {
        disposed = true;
        closeCursor();
      },
    };
  }

  let loaded = false;
  clear(right, emptyState('docs', 'Pick a collection', 'Each one opens with DOC.FIND {} and the first 50 documents.'));
  return {
    el,
    async show(param) {
      if (!loaded) {
        loaded = true;
        await loadCollections();
      }
      const target = param || ns || (colls[0] && colls[0].ns) || '';
      if (target && target !== ns) openCollection(target);
      else if (!target) {
        paintCollections();
        clear(right, emptyState('docs', 'Pick a collection', 'Each one opens with DOC.FIND {} and the first 50 documents.'));
      }
    },
    focusSearch() { view?.focus(); },
    newCollection,
  };
}
