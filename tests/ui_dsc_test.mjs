// 사용법: node tests/ui_dsc_test.mjs (Playwright + Chromium 필요)
// 신규 지표 발굴(A-3): 가짜 세계 보정이 거짓 발견을 막는지(귀무), 심어 둔 신호를 찾는지·방향·결측 처리, 후보 지표 계산, 화면 동작을 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uidsc_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
// 현실적인 가짜 재료: 40종목 × 2015~2024 연간. 종목마다 마진·부채·성장이 다르고 수익률은 무작위(+ 신호 심기 옵션)
let seed = 7; const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
const NS = 60, comps = [], all = {};
const prof = [];
for (let k = 0; k < NS; k++) prof.push({ opm: 0.04 + rnd() * 0.2, lev: 0.2 + rnd() * 1.6, gr: -0.02 + rnd() * 0.15, rd: rnd() });
for (let k = 0; k < NS; k++) {
  const c = 'S' + String(k).padStart(2, '0'); all[c] = []; comps.push({ corp_code: c, corp_name: '종목' + k, stock_code: String(200000 + k), n: 10, acc_mt: 12 });
  const P = prof[k]; let rev = 1000e8 * (0.5 + rnd());
  for (let y = 2015; y <= 2024; y++) {
    rev *= 1 + P.gr + (rnd() - 0.5) * 0.05; const opi = rev * (P.opm + (rnd() - 0.5) * 0.02), ni = opi * 0.75, eq = rev * 1.2, debt = eq * P.lev;
    const pr = 10000 * (1 + 0.1 * rnd());
    all[c].push({ corp_code: c, bsns_year: String(y), reprt_code: '11011', period_label: y + ' 사업보고서', period_order: y * 10 + 4, fs_div: 'CFS', revenue: rev, cogs: rev * 0.7, operating_income: opi, net_income: ni, total_equity: eq, total_liabilities: debt, cash: rev * 0.1, ocf: ni * 1.1, capex: rev * 0.05, fcf: ni * 1.1 - rev * 0.05, total_shares: 1e7, treasury_shares: 0, parent_net_income: ni, parent_equity: eq, current_assets: rev * 0.4, current_liabilities: rev * 0.25, depreciation: rev * 0.03, receivables: rev * 0.12, inventory: rev * 0.08, payables: rev * 0.07, tangible_assets: rev * 0.5, intangible_assets: rev * 0.05, short_term_borrowings: debt * 0.2, long_term_borrowings: debt * 0.3, interest_expense: debt * 0.03, dividends_paid: ni * 0.2, dividend_per_share: ni * 0.2 / 1e7, pretax_income: ni / 0.78, st_financial_assets: rev * 0.02, filing_date: (y + 1) + '0320', price_at_period_end: pr, price_next_day: pr * Math.exp(0.1 * (y - 2015) * 0 + (rnd() - 0.5) * 0.6), price_next_day_date: (y + 1) + '0321' });
  }
}
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: comps }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => all[c] || [])) })); }
  if (p === '/api/stock/list') return r.end(JSON.stringify({ stocks: [] }));
  if (p === '/api/industry/lookup') return r.end('{"map":{}}');
  r.end('{}');
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(500);

// 1) 귀무: 지표와 수익률이 무관 → 보정 후 유의가 거의 없어야 한다(보정 안 하면 5% 안팎이 걸림)
const nul = await pg.evaluate(async () => {
  let rs = 11; const rnd = () => { rs = (rs * 1664525 + 1013904223) >>> 0; return rs / 4294967296; };
  let adj = 0, raw = 0, tot = 0;
  for (let rep = 0; rep < 12; rep++) {
    const keys = Array.from({ length: 40 }, (_, i) => 'k' + i), periods = [];
    for (let t = 0; t < 12; t++) { const items = []; for (let i = 0; i < 60; i++) { const v = {}; keys.forEach((k) => { if (rnd() > 0.15) v[k] = rnd(); }); items.push({ id: 's' + i, f: (rnd() - 0.5) * 0.4, v }); } periods.push({ label: 'P' + t, items }); }
    const R = await dscSearch({ periods, keys, G: 5, B: 200, seed: rep + 1, split: 2 / 3, trim: true, ppy: 1 });
    R.rows.forEach((r) => { if (r.insufficient) return; tot++; if (r.pRaw <= 0.05) raw++; if (r.pAdj <= 0.05) adj++; });
  }
  return { adj, raw, tot };
});
ok('귀무(무관한 지표 40개 × 12회): 보정 전 유의 ' + nul.raw + '개(≈5%) → 보정 후 ' + nul.adj + '개', nul.raw >= 10 && nul.adj <= 2, JSON.stringify(nul));

// 2) 심어 둔 신호: k0=낮을수록 유리, k1=높을수록 유리, k2=결측 55%인 신호, k3=거의 전부 결측
const pl = await pg.evaluate(async () => {
  let rs = 5; const rnd = () => { rs = (rs * 1664525 + 1013904223) >>> 0; return rs / 4294967296; };
  const keys = Array.from({ length: 30 }, (_, i) => 'k' + i), periods = [];
  for (let t = 0; t < 12; t++) { const items = []; for (let i = 0; i < 80; i++) { const v = {}, z = []; keys.forEach((k, j) => { const x = rnd(); z.push(x); if (j === 3 ? rnd() > 0.05 : (j === 2 ? rnd() > 0.45 : false)) return; v[k] = x; });
      const f = (rnd() - 0.5) * 0.3 - 0.25 * (z[0] - 0.5) + 0.25 * (z[1] - 0.5) + 0.25 * (z[2] - 0.5); items.push({ id: 's' + i, f, v }); } periods.push({ label: 'P' + t, items }); }
  const R = await dscSearch({ periods, keys, G: 5, B: 300, seed: 3, split: 2 / 3, trim: false, ppy: 1 });
  const by = {}; R.rows.forEach((r) => { by[r.key] = r; });
  return { k0: by.k0, k1: by.k1, k2: by.k2, k3: by.k3, nOther: R.rows.filter((r) => !['k0', 'k1', 'k2'].includes(r.key) && r.pAdj != null && r.pAdj <= 0.05).length, first: R.rows[0].key };
});
ok('낮을수록 유리 신호(k0) 발견: 평균 스프레드 < 0, 보정 p ≤ 0.05, 확인 구간도 같은 방향', pl.k0.mean < 0 && pl.k0.pAdj <= 0.05 && pl.k0.conf.same, JSON.stringify({ m: pl.k0.mean, p: pl.k0.pAdj }));
ok('높을수록 유리 신호(k1) 발견: 스프레드 > 0, 보정 p ≤ 0.05', pl.k1.mean > 0 && pl.k1.pAdj <= 0.05, JSON.stringify({ m: pl.k1.mean, p: pl.k1.pAdj }));
ok('결측이 많아도(k2, 약 45%만 값 있음) 값이 있는 종목만으로 검정, 종목수 표시가 전체보다 적음', pl.k2.pAdj != null && pl.k2.avgN < 60 && pl.k2.avgN > 20, JSON.stringify({ avgN: pl.k2.avgN, p: pl.k2.pAdj }));
ok('값이 거의 없는 지표(k3)는 검정하지 않고 제외 표시', pl.k3.insufficient === true || pl.k3.pAdj == null || pl.k3.avgN < 20, JSON.stringify(pl.k3).slice(0, 120));
ok('신호 없는 나머지 지표는 보정 후 거의 유의하지 않음(≤1)', pl.nOther <= 1, String(pl.nOther));

// 3) 후보 지표 계산 점검: 저장 종목 → 연간 기간 구성 → 지표별 값이 나오는 종목 수
await pg.evaluate(() => btLoadStocks()); await pg.waitForTimeout(1500);
const cov = await pg.evaluate(() => {
  const keys = Object.keys(BT_FACTORS).concat(Object.keys(BT_CANDS)); const per = btBuildAnnual(keys, { keepMissing: true });
  const n = {}; per.forEach((p) => p.items.forEach((x) => keys.forEach((k) => { if (anFin(x.v[k])) n[k] = (n[k] || 0) + 1; })));
  const bad = []; per.forEach((p) => p.items.forEach((x) => keys.forEach((k) => { if (x.v[k] !== undefined && !anFin(x.v[k])) bad.push(k); })));
  return { periods: per.length, items: per.reduce((a, p) => a + p.items.length, 0), nCand: Object.keys(BT_CANDS).length, zero: keys.filter((k) => !n[k]), bad: bad.length, grp: Array.from(new Set(Object.keys(BT_CANDS).map((k) => BT_CANDS[k].grp))) };
});
ok('후보 지표 40개 이상, 분야 7개', cov.nCand >= 40 && cov.grp.length === 7, JSON.stringify({ n: cov.nCand, g: cov.grp.length }));
ok('기간·종목이 만들어지고 NaN/무한 값이 섞이지 않음', cov.periods >= 8 && cov.items > 300 && cov.bad === 0, JSON.stringify({ p: cov.periods, i: cov.items, bad: cov.bad }));
ok('모든 지표(기존+후보)가 계정이 갖춰진 재료에서는 값이 계산됨(0건 지표: ' + cov.zero.join(',') + ')', cov.zero.length === 0, cov.zero.join(','));

// 4) 화면 실행
await pg.selectOption('#dscMode', 'annual'); await pg.selectOption('#dscB', '200'); await pg.click('button:has-text("신규 지표 발굴 실행")'); await pg.waitForTimeout(6000);
const ui = await pg.evaluate(() => ({ rows: document.querySelectorAll('#dscOut tbody tr').length, txt: document.getElementById('dscOut').innerText.slice(0, 400), msg: document.getElementById('dscMsg').innerText, newN: Array.prototype.filter.call(document.querySelectorAll('#dscOut tbody tr'), (r) => r.children[1] && r.children[1].textContent === '신규').length, oldN: Array.prototype.filter.call(document.querySelectorAll('#dscOut tbody tr'), (r) => r.children[1] && r.children[1].textContent === '기존').length }));
ok('결과 표: 기존 10 + 신규 40여 행, 안내·판정 문구 표시', ui.oldN === 10 && ui.newN >= 40 && ui.txt.includes('우연의 기준선') && ui.txt.includes('판정'), JSON.stringify({ o: ui.oldN, n: ui.newN, msg: ui.msg }) + ui.txt.slice(0, 80));
// 열기: 방향 반영해 정식 백테스트 실행
const idx = await pg.evaluate(() => DSC.res.rows.findIndex((r) => !r.insufficient && btFac(r.key).cand));
await pg.evaluate((i) => dscOpen(i), idx); await pg.waitForTimeout(1200);
const op = await pg.evaluate((i) => { const r = DSC.res.rows[i]; return { key: r.key, neg: r.mean < 0, dir: BT_DIR[r.key] === -1, checked: Array.prototype.filter.call(document.querySelectorAll('.btFac'), (c) => c.checked).map((c) => c.value).join(), out: document.getElementById('btFinOut').innerText.slice(0, 200), cand: !!document.getElementById('btCandBox') }; }, idx);
ok('열기: 그 지표만 체크되어 A 백테스트 실행, 낮을수록 유리면 값 뒤집힘 표시', op.checked === op.key && op.cand && op.dir === op.neg && op.out.includes('판정') && (!op.neg || op.out.includes('값 뒤집음')), JSON.stringify(op).slice(0, 220));
// 기존 방법 실행이 여전히 동일하게 동작(값 뒤집힘이 다른 키에 새지 않음)
await pg.evaluate(() => { Object.keys(BT_DIR).forEach((k) => delete BT_DIR[k]); document.querySelectorAll('.btFac').forEach((c) => { c.checked = c.value === 'ep'; }); return btRunFin(); }); await pg.waitForTimeout(800);
ok('기존 방법(EP) 백테스트는 그대로 동작', (await pg.innerText('#btFinOut')).includes('판정'));
ok('페이지 오류 없음', errs.length === 0, errs.join(';'));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
