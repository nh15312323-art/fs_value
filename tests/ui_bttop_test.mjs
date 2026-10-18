// 사용법: node tests/ui_bttop_test.mjs (Playwright + Chromium 필요)
// ⑤ 백테스트 결과의 「상위 그룹에 들어간 종목 상세」: 기간별 상위 그룹 종목·점수·수익률 표, 기간 선택, 종목명 클릭 시 C 카드 연결, CSV를 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uibttop_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const NS = 16;
const mk = (code, y, ni, pr) => ({ corp_code: code, bsns_year: String(y), reprt_code: '11011', period_label: y + ' 사업보고서', period_order: y * 10 + 4, fs_div: 'CFS', revenue: 1000e8, cogs: 600e8, operating_income: 150e8, net_income: ni, total_equity: 2000e8, total_liabilities: 800e8, cash: 300e8, ocf: 180e8, capex: 60e8, fcf: 120e8, total_shares: 1e7, treasury_shares: 0, parent_net_income: ni, parent_equity: 2000e8, filing_date: (y + 1) + '0320', price_at_period_end: pr, price_next_day: pr, price_next_day_date: (y + 1) + '0321' });
const all = {};
for (let k = 0; k < NS; k++) { const c = 'C' + String(k).padStart(2, '0'); all[c] = []; for (let y = 2019; y <= 2023; y++) { const pr = 10000 * (1 + 0.05 * k + 0.02 * (y - 2019)); all[c].push(mk(c, y, (50 + k * 6) * 1e8, pr)); } }
const comps = Object.keys(all).map((c, i) => ({ corp_code: c, corp_name: '종목' + String.fromCharCode(65 + i), stock_code: String(100000 + i), n: 5, acc_mt: 12 }));
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: comps }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => all[c] || [])) })); }
  if (p === '/api/stock/list') return r.end(JSON.stringify({ stocks: [] }));
  if (p === '/api/db/usage') return r.end(JSON.stringify({ sizeBytes: 1e6, limitBytes: 5e8, limitMb: 500, warnPct: 90, pct: 0.2, warn: false }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, acceptDownloads: true });
const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(400);
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(700);
await pg.evaluate(() => btLoadStocks()); await pg.waitForTimeout(1500);
await pg.evaluate(() => { document.querySelectorAll('.btFac').forEach((c) => { c.checked = c.value === 'ep'; }); return btRunFin(); }); await pg.waitForTimeout(800);
const out = await pg.innerText('#btFinOut');
ok('결과에 「상위 그룹에 들어간 종목 상세」 표시', out.includes('상위 그룹에 들어간 종목 상세'), out.slice(0, 60).replace(/\n/g, ' '));
const info = await pg.evaluate(() => { const sel = document.getElementById('btTopSel_fin'); const tr = document.querySelectorAll('#btTopBody_fin tbody tr'); return { opts: sel ? sel.options.length : -1, cur: sel ? sel.options[sel.selectedIndex].text : '', n: tr.length, first: tr[0] ? tr[0].innerText.replace(/\s+/g, ' ') : '', head: Array.from(document.querySelectorAll('#btTopBody_fin thead th')).map((t) => t.innerText).join('|') }; });
ok('기간 선택 4개(FY2019~2022), 기본은 최신 기간', info.opts === 4 && info.cur.indexOf('FY2022') === 0, JSON.stringify(info));
const sz = await pg.evaluate(() => ({ n: BT_TOP.fin.res.rows[BT_TOP.fin.res.rows.length - 1].n, G: BT_TOP.fin.res.G, top: BT_TOP.fin.res.rows[BT_TOP.fin.res.rows.length - 1].top.length, tot: BT_TOP.fin.res.rows.reduce((a, r) => a + r.top.length, 0) }));
ok('상위 그룹 크기 = 마지막 그룹(전체 ÷ G 범위), 표 행 수와 일치', info.n === sz.top && sz.top >= Math.floor(sz.n / sz.G) && sz.top <= Math.ceil(sz.n / sz.G), JSON.stringify(sz));
ok('열: 순위·종목·코드·점수·EP(이익수익률)·수익률', ['순위', '종목', '코드', '점수', 'EP', '수익률'].every((t) => info.head.includes(t)), info.head);
// EP = EPS/주가. 이익이 클수록, 주가가 낮을수록 높다. k가 클수록 이익은 크고 주가도 높으나 이익 증가(6k)가 가격 증가(5%k)보다 상대적으로 커서 가장 높은 k가 1순위인지 계산으로 확인
const expect = await pg.evaluate(() => { const r = BT_TOP.fin.res.rows[BT_TOP.fin.res.rows.length - 1]; const s = r.top.map((z) => z.s); return { sorted: s.every((v, i) => i === 0 || s[i - 1] >= v), n: r.top.length, hasV: r.top.every((z) => z.v && typeof z.v.ep === 'number') }; });
ok('상위 종목은 점수 내림차순이고 팩터 값이 함께 저장됨', expect.sorted && expect.hasV, JSON.stringify(expect));
await pg.selectOption('#btTopSel_fin', '0'); await pg.waitForTimeout(200);
const lab = await pg.innerText('#btTopBody_fin');
ok('기간을 바꾸면 해당 기간 요약(FY2019 → 2020)으로 갱신', lab.includes('FY2019'), lab.slice(0, 80).replace(/\n/g, ' '));
// 종목명 클릭 -> C 카드
const name = await pg.evaluate(() => document.querySelector('#btTopBody_fin tbody tr td:nth-child(2) a').innerText);
await pg.click('#btTopBody_fin tbody tr td:nth-child(2) a'); await pg.waitForTimeout(900);
const stk = await pg.innerText('#btStkOut');
ok('종목명을 누르면 C 카드에 그 종목의 연도별 표가 열림', stk.includes(name) && stk.includes('결산연도'), name + ' / ' + stk.slice(0, 50).replace(/\n/g, ' '));
// CSV
const [dl] = await Promise.all([pg.waitForEvent('download'), pg.click('button:has-text("전체 기간 CSV")')]);
const csv = fs.readFileSync(await dl.path(), 'utf8'); const lines = csv.replace(/^﻿/, '').split('\n');
ok('CSV: BOM, 머리글 + 기간별 상위 종목 전부, 종목명 포함', csv.charCodeAt(0) === 0xFEFF && lines.length === 1 + sz.tot && lines[0].startsWith('기간,순위,종목,코드,점수,수익률') && /종목[A-P]/.test(lines[1]), String(lines.length) + ' ' + lines[1]);
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
