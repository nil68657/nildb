// Geo: GEO sets and DOC.* 2dsphere fields on an offline world map. Click
// the map to search a radius around the point: GEOSEARCH for a GEO set,
// $near or $geoWithin for documents.

import {
  h, clear, api, text, num, items, isErr, toJS, fmtInt, fmtNum, iconButton, button, svgEl, ejsonNumber,
  ejsonLabel, quoteArg, debounce, ReplyError,
} from '../lib.js';
import { toastError, seg, spinner } from '../ui.js';
import { LAND } from '../world.js';

// Earth radii: NilDB's 2dsphere code uses MongoDB's, GEO sets use Redis's.
const R_2DSPHERE = 6378100;
const R_REDIS = 6372797.560856;
const PLOT_MAX = 5000;
// MIN_VIEW is the narrowest map view in degrees of longitude, about 10 km:
// enough to tell the points of one neighbourhood apart.
const MIN_VIEW = 360 / 4096;
const rad = (d) => (d * Math.PI) / 180;
const deg = (r) => (r * 180) / Math.PI;

function haversine(lon1, lat1, lon2, lat2, R) {
  const a = Math.sin(rad(lat2 - lat1) / 2) ** 2 + Math.cos(rad(lat1)) * Math.cos(rad(lat2)) * Math.sin(rad(lon2 - lon1) / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

// circlePath is a geodesic circle as an SVG path in map coordinates,
// broken where it crosses the antimeridian.
function circlePath(lon, lat, metres, R) {
  const d = metres / R;
  const out = [];
  let prevX = null;
  let broken = false;
  for (let i = 0; i <= 96; i++) {
    const b = rad((i / 96) * 360);
    const lat2 = Math.asin(Math.sin(rad(lat)) * Math.cos(d) + Math.cos(rad(lat)) * Math.sin(d) * Math.cos(b));
    const lon2 = rad(lon) + Math.atan2(Math.sin(b) * Math.sin(d) * Math.cos(rad(lat)), Math.cos(d) - Math.sin(rad(lat)) * Math.sin(lat2));
    const x = (((deg(lon2) + 540) % 360) + 360) % 360;
    const y = 90 - deg(lat2);
    const jump = prevX !== null && Math.abs(x - prevX) > 180;
    if (jump) broken = true;
    out.push(`${prevX === null || jump ? 'M' : 'L'}${x.toFixed(5)} ${y.toFixed(5)}`);
    prevX = x;
  }
  return out.join('') + (broken ? '' : 'Z');
}

const dots = (pts) => pts.map((p) => `M${(p.lon + 180).toFixed(5)} ${(90 - p.lat).toFixed(5)}h0`).join('');
const fmtKm = (m) => (m < 1000 ? `${Math.round(m)} m` : `${fmtNum(m / 1000, m < 10000 ? 2 : 1)} km`);
const fmtCoord = (p) => `${p.lat.toFixed(5)}, ${p.lon.toFixed(5)}`;

const GRATICULE = (() => {
  let d = '';
  for (let y = 30; y < 180; y += 30) d += `M0 ${y}H360`;
  for (let x = 30; x < 360; x += 30) d += `M${x} 0V180`;
  return d;
})();

class WorldMap {
  constructor({ onClick }) {
    this.onClick = onClick;
    this.points = [];
    this.hits = [];
    this.view = { x: 0, y: 0, w: 360, h: 180 };
    this.svg = svgEl(`<svg class="map" viewBox="0 0 360 180" role="application" tabindex="0" aria-label="World map. Click or press Enter to search around a point; plus and minus zoom; arrows pan.">
      <path class="graticule" d="${GRATICULE}"/><path class="land" d="${LAND}"/>
      <path class="circle"/><path class="pts"/><path class="pts hit"/><path class="pts sel"/><path class="center"/></svg>`);
    [this.circle, this.ptsEl, this.hitEl, this.selEl, this.centerEl] = ['.circle', '.pts:not(.hit):not(.sel)', '.pts.hit', '.pts.sel', '.center']
      .map((s) => this.svg.querySelector(s));
    this.tip = h('div', { class: 'map-tip', hidden: true });
    this.coords = h('div', { class: 'map-coords', 'aria-hidden': 'true' }, 'lat, lon');
    this.el = h('div', { class: 'map-wrap' }, this.svg, this.tip, this.coords,
      h('div', { class: 'map-ctrl' },
        iconButton('zoomIn', 'Zoom in', () => this.zoomBy(0.6)),
        iconButton('zoomOut', 'Zoom out', () => this.zoomBy(1 / 0.6)),
        iconButton('frame', 'Fit the plotted points', () => this.fit()),
        iconButton('geo', 'Whole world', () => this.home())),
      h('div', { class: 'map-attr' }, 'Land: Natural Earth, public domain'));
    this.bind();
    new ResizeObserver(() => this.apply()).observe(this.el);
  }

  bind() {
    let drag = null;
    this.svg.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return;
      drag = { x: e.clientX, y: e.clientY, view: { ...this.view }, moved: false };
      try {
        this.svg.setPointerCapture(e.pointerId);
      } catch {
        // A pointer that is not active cannot be captured; the drag still works inside the map.
      }
    });
    this.svg.addEventListener('pointermove', (e) => {
      const p = this.toMap(e.clientX, e.clientY);
      this.coords.textContent = `${(90 - p.y).toFixed(4)}, ${(p.x - 180).toFixed(4)}`;
      if (drag) {
        const dx = e.clientX - drag.x;
        const dy = e.clientY - drag.y;
        if (!drag.moved && Math.hypot(dx, dy) > 4) {
          drag.moved = true;
          this.svg.classList.add('panning');
          this.tip.hidden = true;
        }
        if (drag.moved) {
          const k = this.view.w / this.svg.clientWidth;
          this.view.x = drag.view.x - dx * k;
          this.view.y = drag.view.y - dy * k;
          this.apply();
        }
        return;
      }
      this.hover(e.clientX, e.clientY);
    });
    this.svg.addEventListener('pointerup', (e) => {
      if (!drag) return;
      const wasDrag = drag.moved;
      drag = null;
      this.svg.classList.remove('panning');
      if (!wasDrag) {
        const p = this.toMap(e.clientX, e.clientY);
        this.onClick(p.x - 180, 90 - p.y);
      }
    });
    this.svg.addEventListener('pointerleave', () => { this.tip.hidden = true; });
    this.svg.addEventListener('wheel', (e) => {
      e.preventDefault();
      this.zoomAt(e.clientX, e.clientY, Math.exp(e.deltaY * 0.0015));
    }, { passive: false });
    this.svg.addEventListener('keydown', (e) => {
      const step = this.view.w * 0.1;
      const keys = {
        '+': () => this.zoomBy(0.7), '=': () => this.zoomBy(0.7), '-': () => this.zoomBy(1 / 0.7), 0: () => this.home(),
        ArrowLeft: () => { this.view.x -= step; }, ArrowRight: () => { this.view.x += step; },
        ArrowUp: () => { this.view.y -= step; }, ArrowDown: () => { this.view.y += step; },
        Enter: () => this.onClick(this.view.x + this.view.w / 2 - 180, 90 - (this.view.y + this.view.h / 2)),
      };
      if (!keys[e.key]) return;
      e.preventDefault();
      keys[e.key]();
      this.apply();
    });
  }

  toMap(cx, cy) {
    const m = this.svg.getScreenCTM();
    if (!m) return { x: 180, y: 90 };
    const p = new DOMPoint(cx, cy).matrixTransform(m.inverse());
    return { x: p.x, y: p.y };
  }

  apply() {
    const cw = this.svg.clientWidth || 800;
    const ch = this.svg.clientHeight || 400;
    const v = this.view;
    v.w = Math.min(Math.max(v.w, MIN_VIEW), 720);
    v.h = (v.w * ch) / cw;
    const cx = Math.min(Math.max(v.x + v.w / 2, 0), 360);
    const cy = Math.min(Math.max(v.y + v.h / 2, 0), 180);
    v.x = cx - v.w / 2;
    v.y = cy - v.h / 2;
    this.svg.setAttribute('viewBox', `${v.x.toFixed(4)} ${v.y.toFixed(4)} ${v.w.toFixed(4)} ${v.h.toFixed(4)}`);
  }

  zoomAt(clientX, clientY, f) {
    const p = this.toMap(clientX, clientY);
    const v = this.view;
    const nw = Math.min(Math.max(v.w * f, MIN_VIEW), 720);
    const k = nw / v.w;
    v.x = p.x - (p.x - v.x) * k;
    v.y = p.y - (p.y - v.y) * k;
    v.w = nw;
    this.apply();
  }

  zoomBy(f) {
    const r = this.svg.getBoundingClientRect();
    this.zoomAt(r.left + r.width / 2, r.top + r.height / 2, f);
  }

  box(x0, y0, x1, y1) {
    const cw = this.svg.clientWidth || 800;
    const ch = this.svg.clientHeight || 400;
    const w = Math.max(x1 - x0, ((y1 - y0) * cw) / ch, MIN_VIEW);
    this.view = { x: (x0 + x1) / 2 - w / 2, y: (y0 + y1) / 2 - (w * ch) / cw / 2, w, h: 0 };
    this.apply();
  }

  home() {
    const cw = this.svg.clientWidth || 800;
    const ch = this.svg.clientHeight || 400;
    if (ch / cw > 0.5) this.box(0, 0, 360, 180);
    else this.box(180 - (180 * cw) / ch / 2, 0, 180 + (180 * cw) / ch / 2, 180);
  }

  fit(pts = this.points) {
    if (!pts.length) {
      this.home();
      return;
    }
    let x0 = 360; let y0 = 180; let x1 = 0; let y1 = 0;
    for (const p of pts) {
      x0 = Math.min(x0, p.lon + 180); x1 = Math.max(x1, p.lon + 180);
      y0 = Math.min(y0, 90 - p.lat); y1 = Math.max(y1, 90 - p.lat);
    }
    const pad = Math.max(0.05, (x1 - x0) * 0.12, (y1 - y0) * 0.12);
    this.box(x0 - pad, y0 - pad, x1 + pad, y1 + pad);
  }

  hover(cx, cy) {
    const all = this.hits.length ? this.hits.concat(this.points) : this.points;
    if (!all.length) return;
    const p = this.toMap(cx, cy);
    const lim = (9 * this.view.w) / this.svg.clientWidth;
    let best = null;
    let bd = lim;
    for (const q of all) {
      const d = Math.hypot(q.lon + 180 - p.x, 90 - q.lat - p.y);
      if (d < bd) { bd = d; best = q; }
    }
    if (!best) {
      this.tip.hidden = true;
      return;
    }
    const r = this.el.getBoundingClientRect();
    this.tip.textContent = best.dist != null ? `${best.label} · ${fmtKm(best.dist)}` : best.label;
    this.tip.style.left = `${cx - r.left}px`;
    this.tip.style.top = `${cy - r.top}px`;
    this.tip.hidden = false;
  }

  setPoints(pts) {
    this.points = pts;
    this.ptsEl.setAttribute('d', dots(pts));
  }

  setHits(pts) {
    this.hits = pts;
    this.hitEl.setAttribute('d', dots(pts));
  }

  select(p) {
    this.selEl.setAttribute('d', p ? dots([p]) : '');
    if (!p) return;
    const x = p.lon + 180;
    const y = 90 - p.lat;
    const v = this.view;
    if (x < v.x || x > v.x + v.w || y < v.y || y > v.y + v.h) {
      v.x = x - v.w / 2;
      v.y = y - v.h / 2;
      this.apply();
    }
  }

  setCircle(lon, lat, metres, R) {
    this.circle.setAttribute('d', circlePath(lon, lat, metres, R));
    this.centerEl.setAttribute('d', dots([{ lon, lat }]));
  }

  clearSearch() {
    this.circle.removeAttribute('d');
    this.centerEl.removeAttribute('d');
    this.setHits([]);
    this.select(null);
  }
}

// pointOf reads a GeoJSON Point or a legacy [lon, lat] pair from a
// canonical Extended JSON value.
function pointOf(v) {
  if (Array.isArray(v) && v.length >= 2) return [ejsonNumber(v[0]), ejsonNumber(v[1])];
  if (v && typeof v === 'object' && v.type === 'Point' && Array.isArray(v.coordinates)) {
    return [ejsonNumber(v.coordinates[0]), ejsonNumber(v.coordinates[1])];
  }
  return null;
}

function dig(doc, path) {
  let cur = doc;
  for (const part of path.split('.')) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = cur[part];
  }
  return cur;
}

export function create(app) {
  let source = 'geo';
  let center = null;
  let hits = [];

  const map = new WorldMap({ onClick: (lon, lat) => search(lon, lat) });

  const keyList = h('datalist', { id: 'geo-keys' });
  const keyIn = h('input', { class: 'input mono', list: 'geo-keys', placeholder: 'GEO set key, such as geo:cafes', 'aria-label': 'GEO set key', spellcheck: false });
  const keyLabel = h('span', null, `GEO set in db ${app.db}`);
  const geoForm = h('div', { class: 'stack' }, h('label', { class: 'field' }, keyLabel, keyIn), keyList);

  const collSel = h('select', { class: 'select', 'aria-label': 'Collection' });
  const fieldList = h('datalist', { id: 'geo-fields' });
  const fieldIn = h('input', { class: 'input mono', list: 'geo-fields', placeholder: 'loc', 'aria-label': 'Location field', spellcheck: false });
  const labelIn = h('input', { class: 'input mono', value: 'name', 'aria-label': 'Label field', spellcheck: false });
  let docMode = 'near';
  const modeSeg = seg([['near', '$near'], ['within', '$geoWithin']], docMode, (m) => {
    docMode = m;
    if (center) search(center.lon, center.lat);
  }, 'Query operator');
  const docForm = h('div', { class: 'stack', hidden: true },
    h('label', { class: 'field' }, h('span', null, 'Collection'), collSel),
    h('div', { class: 'row' },
      h('label', { class: 'field' }, h('span', null, 'Location field'), fieldIn),
      h('label', { class: 'field' }, h('span', null, 'Label field'), labelIn)),
    fieldList,
    h('div', { class: 'row' }, h('span', { class: 'faint' }, 'Operator'), modeSeg));

  const radiusIn = h('input', { class: 'input mono', type: 'number', min: '0.01', step: 'any', value: '5', 'aria-label': 'Radius in kilometres' });
  const limitIn = h('input', { class: 'input mono', type: 'number', min: '1', step: '1', value: '100', 'aria-label': 'Result limit' });
  const loadBtn = button('Plot', () => load(), { cls: 'sm primary', iconName: 'pin' });
  const info = h('div', { class: 'faint', 'aria-live': 'polite' }, 'Pick a GEO set or a collection, then click the map.');
  const cmdBox = h('div', { class: 'geo-cmd', hidden: true });
  const results = h('div', { class: 'geo-results', role: 'listbox', 'aria-label': 'Search results' });
  const srcSeg = seg([['geo', 'GEO set'], ['docs', 'Documents']], source, (s) => setSource(s), 'Source');

  const side = h('div', { class: 'geo-side' },
    h('div', { class: 'geo-form' },
      srcSeg, geoForm, docForm,
      h('div', { class: 'row' },
        h('label', { class: 'field' }, h('span', null, 'Radius, km'), radiusIn),
        h('label', { class: 'field' }, h('span', null, 'Limit'), limitIn)),
      h('div', { class: 'row' }, loadBtn, button('Clear search', () => {
        center = null;
        hits = [];
        map.clearSearch();
        paintHits();
      }, { cls: 'sm ghost' })),
      info, cmdBox),
    results);
  const el = h('section', { class: 'view', 'aria-label': 'Geo' },
    h('div', { class: 'view-head' }, h('h1', null, 'Geo'), h('span', { class: 'sub' }, 'Click the map to search around a point; drag to pan, scroll to zoom')),
    h('div', { class: 'geo-main' }, map.el, side));

  function setSource(s) {
    source = s;
    srcSeg.set(s);
    geoForm.hidden = s !== 'geo';
    docForm.hidden = s !== 'docs';
    center = null;
    hits = [];
    map.clearSearch();
    paintHits();
  }

  function showCmd(args) {
    cmdBox.hidden = false;
    cmdBox.textContent = args.map(quoteArg).join(' ');
  }

  async function load() {
    loadBtn.disabled = true;
    clear(info, spinner(), ' plotting');
    try {
      let pts;
      let total;
      let from;
      if (source === 'geo') {
        const key = keyIn.value.trim();
        if (!key) throw new Error('Type the key of a GEO set');
        const args = ['GEOSEARCH', key, 'FROMLONLAT', '0', '0', 'BYRADIUS', '20100', 'km', 'WITHCOORD', 'COUNT', String(PLOT_MAX), 'ANY'];
        const [card, r] = await api.batch([['ZCARD', key], args]);
        if (isErr(r)) throw new ReplyError(r.v);
        total = num(card);
        pts = items(r).map((row) => {
          const [m, c] = items(row);
          const [lon, lat] = items(c).map(num);
          return { label: text(m), lon, lat };
        });
        from = `GEO set ${key}`;
        showCmd(args);
      } else {
        const ns = collSel.value;
        const f = fieldIn.value.trim();
        if (!ns || !f) throw new Error('Pick a collection and its location field');
        const filter = JSON.stringify({ [f]: { $exists: true } });
        const proj = JSON.stringify({ [f]: 1, [labelIn.value.trim() || '_id']: 1 });
        const args = ['DOC.FIND', ns, filter, 'PROJECT', proj, 'LIMIT', String(-PLOT_MAX)];
        const [r, cnt] = await api.batch([args, ['DOC.COUNT', ns, filter]]);
        if (isErr(r)) throw new ReplyError(r.v);
        total = isErr(cnt) ? null : num(cnt);
        pts = docPoints(toJS(r).cursor.firstBatch.map((s) => JSON.parse(s)));
        from = `${ns} field ${f}`;
        showCmd(args);
      }
      map.setPoints(pts);
      map.fit(pts);
      info.textContent = `${fmtInt(pts.length)}${total != null && total > pts.length ? ` of ${fmtInt(total)}` : ''} points from ${from}. Click the map to search.`;
      center = null;
      hits = [];
      map.clearSearch();
      paintHits();
    } catch (e) {
      clear(info, h('span', { class: 'r-err' }, e.message));
    } finally {
      loadBtn.disabled = false;
    }
  }

  function docPoints(docs) {
    const f = fieldIn.value.trim();
    const lf = labelIn.value.trim() || '_id';
    const out = [];
    for (const d of docs) {
      const p = pointOf(dig(d, f));
      if (!p || !Number.isFinite(p[0]) || !Number.isFinite(p[1])) continue;
      const lv = dig(d, lf);
      out.push({ label: lv === undefined ? ejsonLabel(d._id) : ejsonLabel(lv), lon: p[0], lat: p[1] });
    }
    return out;
  }

  async function search(lon, lat) {
    lon = Number((((lon + 540) % 360) - 180).toFixed(6));
    lat = Number(Math.max(-85.05112878, Math.min(85.05112878, lat)).toFixed(6));
    const km = Number(radiusIn.value);
    const limit = Math.max(1, Math.floor(Number(limitIn.value) || 100));
    if (!(km > 0)) {
      toastError(new Error('The radius must be above 0 km'));
      return;
    }
    center = { lon, lat };
    map.setCircle(lon, lat, km * 1000, source === 'geo' ? R_REDIS : R_2DSPHERE);
    clear(results, h('div', { class: 'empty' }, spinner(), 'Searching'));
    try {
      if (source === 'geo') {
        const key = keyIn.value.trim();
        if (!key) throw new Error('Type the key of a GEO set first');
        const args = ['GEOSEARCH', key, 'FROMLONLAT', lon.toFixed(6), lat.toFixed(6), 'BYRADIUS', String(km), 'km', 'WITHCOORD', 'WITHDIST', 'ASC', 'COUNT', String(limit)];
        showCmd(args);
        const r = await api.ok(args);
        hits = items(r).map((row) => {
          const [m, d, c] = items(row);
          const [plon, plat] = items(c).map(num);
          return { label: text(m), dist: num(d) * 1000, lon: plon, lat: plat };
        });
      } else {
        const ns = collSel.value;
        const f = fieldIn.value.trim();
        if (!ns || !f) throw new Error('Pick a collection and its location field first');
        const point = { type: 'Point', coordinates: [lon, lat] };
        const filter = docMode === 'near'
          ? { [f]: { $near: { $geometry: point, $maxDistance: km * 1000 } } }
          : { [f]: { $geoWithin: { $centerSphere: [[lon, lat], (km * 1000) / R_2DSPHERE] } } };
        const proj = JSON.stringify({ [f]: 1, [labelIn.value.trim() || '_id']: 1 });
        const args = ['DOC.FIND', ns, JSON.stringify(filter), 'PROJECT', proj, 'LIMIT', String(-limit)];
        showCmd(args);
        const r = await api.ok(args);
        hits = docPoints(toJS(r).cursor.firstBatch.map((s) => JSON.parse(s)))
          .map((p) => ({ ...p, dist: haversine(lon, lat, p.lon, p.lat, R_2DSPHERE) }));
        if (docMode === 'within') hits.sort((a, b) => a.dist - b.dist);
      }
      map.setHits(hits);
      paintHits();
    } catch (e) {
      clear(results, h('div', { class: 'empty' }, h('span', { class: 'r-err' }, e.message)));
    }
  }

  function paintHits() {
    if (!center) {
      clear(results);
      return;
    }
    const how = source === 'docs' ? (docMode === 'near' ? '$near, nearest first' : '$geoWithin, sorted here by distance') : 'GEOSEARCH, nearest first';
    const head = h('div', { class: 'pane-foot' }, `${fmtInt(hits.length)} within ${fmtNum(Number(radiusIn.value))} km of ${fmtCoord(center)} · ${how}`);
    if (!hits.length) {
      clear(results, head, h('div', { class: 'empty' }, 'Nothing in this circle. Widen the radius or click elsewhere.'));
      return;
    }
    clear(results, head, hits.map((p, i) => h('div', {
      class: 'geo-hit', role: 'option', 'aria-selected': 'false', tabindex: 0,
      onClick: (e) => pick(e.currentTarget, p),
      onKeydown: (e) => { if (e.key === 'Enter') pick(e.currentTarget, p); },
    }, h('span', { class: 'n', title: p.label }, `${i + 1}. ${p.label}`), h('span', { class: 'd' }, fmtKm(p.dist)), h('span', { class: 'c' }, fmtCoord(p)))));
  }

  function pick(row, p) {
    results.querySelectorAll('.geo-hit').forEach((r) => r.setAttribute('aria-selected', String(r === row)));
    map.select(p);
  }

  // discover fills the key and collection pickers and, when autoload is
  // set, plots the first GEO set or 2dsphere collection it finds.
  async function discover(autoload) {
    try {
      const [scan, colls] = await Promise.all([
        api.scan({ db: app.db, type: 'zset', count: 200 }),
        api.run(['DOC.COLLECTIONS']),
      ]);
      clear(keyList, scan.keys.map((k) => h('option', { value: k.k })));
      const names = items(colls).map(text).slice(0, 100);
      const ix = names.length ? await api.batch(names.map((n) => ['DOC.INDEXES', n])) : [];
      const geoColls = [];
      names.forEach((n, i) => {
        if (isErr(ix[i])) return;
        const fields = toJS(ix[i]).filter((x) => x.kind === '2dsphere').flatMap((x) => Object.keys(JSON.parse(x.key)));
        if (fields.length) geoColls.push({ ns: n, fields });
      });
      const rest = names.filter((n) => !geoColls.some((g) => g.ns === n));
      clear(collSel,
        geoColls.map((g) => h('option', { value: g.ns }, `${g.ns} (2dsphere on ${g.fields.join(', ')})`)),
        rest.map((n) => h('option', { value: n }, n)));
      const setFields = () => {
        const g = geoColls.find((x) => x.ns === collSel.value);
        clear(fieldList, (g ? g.fields : []).map((f) => h('option', { value: f })));
        if (g) fieldIn.value = g.fields[0];
      };
      collSel.onchange = setFields;
      setFields();
      if (!autoload) return;
      if (!keyIn.value) {
        const guess = scan.keys.find((k) => /geo|loc|place|point|city|cafe|shop|store/i.test(k.k));
        if (guess) keyIn.value = guess.k;
      }
      if (keyIn.value) {
        setSource('geo');
        load();
      } else if (geoColls.length) {
        setSource('docs');
        load();
      }
    } catch (e) {
      info.textContent = `Could not look for GEO data: ${e.message}`;
    }
  }

  keyIn.addEventListener('keydown', (e) => { if (e.key === 'Enter') load(); });
  fieldIn.addEventListener('keydown', (e) => { if (e.key === 'Enter') load(); });
  radiusIn.addEventListener('input', debounce(() => { if (center) search(center.lon, center.lat); }, 400));
  app.on('db', () => {
    keyLabel.textContent = `GEO set in db ${app.db}`;
    if (source !== 'geo') return;
    map.setPoints([]);
    map.clearSearch();
    center = null;
    hits = [];
    paintHits();
    info.textContent = `Database ${app.db}: type a GEO set key and press Plot.`;
    discover(false);
  });

  let first = true;
  return {
    el,
    show() {
      requestAnimationFrame(() => map.apply());
      if (first) {
        first = false;
        requestAnimationFrame(() => map.home());
        discover(true);
      }
    },
    focusSearch() { (source === 'geo' ? keyIn : fieldIn).focus(); },
    openSet(k) {
      const wasFirst = first;
      first = false;
      requestAnimationFrame(() => map.home());
      keyIn.value = k.b64 ? '' : k.k;
      setSource('geo');
      load();
      if (wasFirst) discover(false);
    },
  };
}
