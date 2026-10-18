// 사용법: node tests/ui_btq_test.mjs (Playwright + Chromium 필요)
// ⑤ 백테스트 A: 「분기 TTM 재무 방법」. 분기보고서마다 최근 4개 분기 합산(TTM) 재무로 방법 값을 계산하고, 공시 다음 거래일 종가 → 다음 분기보고서 공시 다음 거래일 종가 수익률을 쓰는지 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uibtq_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const NS = 16;
const FILE = { 1: '0515', 2: '0814', 3: '1114' };
// 종목 k: 분기 순이익 (10+k)억(매 분기 동일), 주식수 1천만 → TTM EPS = 4*(10+k)*1e8/1e7. 주가는 종목·보고서마다 다르게.
const px = (k, Y, N) => 10000 + 500 * k + 300 * (Y - 2021) * 4 + 300 * N + (k % 3) * 50 * N;
const mk = (code, k, Y, N) => {
  const rc = { 1: '11013', 2: '11012', 3: '11014', 4: '11011' }[N], q = (10 + k) * 1e8, cf = N === 4 ? 4 : N; // 손익: 분기 단독(연간은 누적), 현금흐름: 누적
  const filing = N === 4 ? (Y + 1) + '0331' : Y + FILE[N];
  const nxt = N === 4 ? (Y + 1) + '0401' : Y + FILE[N].replace(/..$/, (d) => String(Number(d) + 1).padStart(2, '0'));
  return { corp_code: code, bsns_year: String(Y), reprt_code: rc, period_label: Y + ' ' + N, period_order: Y * 10 + N, fs_div: 'CFS', revenue: (N === 4 ? 4 : 1) * 500e8, cogs: (N === 4 ? 4 : 1) * 300e8, operating_income: (N === 4 ? 4 : 1) * q * 1.3, net_income: (N === 4 ? 4 : 1) * q, parent_net_income: (N === 4 ? 4 : 1) * q, total_equity: 2000e8, parent_equity: 2000e8, total_liabilities: 800e8, ocf: cf * q * 1.5, capex: cf * q * 0.5, fcf: cf * q, total_shares: 1e7, treasury_shares: 0, filing_date: filing, price_at_period_end: px(k, Y, N), price_next_day: px(k, Y, N), price_next_day_date: nxt };
};
const all = {}; const comps = [];
for (let k = 0; k < NS; k++) { const c = 'Q' + String(k).padStart(2, '0'); all[c] = []; for (let Y = 2021; Y <= 2023; Y++) for (let N = 1; N <= 4; N++) all[c].push(mk(c, k, Y, N)); comps.push({ corp_code: c, corp_name: '분기' + String.fromCharCode(65 + k), stock_code: String(200000 + k), n: 12, acc_mt: 12 }); }
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
const ctx = await b.newContext({ viewport: { width: 1280, height: 900 } });
const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(400);
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(700);
await pg.evaluate(() => btLoadStocks()); await pg.waitForTimeout(1500);
ok('방식 선택에 「분기 TTM 재무 방법」이 있음', await pg.evaluate(() => Array.from(document.getElementById('btFinMode').options).some((o) => o.value === 'qttm')));
const info = await pg.evaluate(() => {
  const per = btBuildQuarterly(['ep']);
  const c03 = per.map((p) => ({ label: p.label, n: p.items.length, it: p.items.filter((z) => z.id === 'Q03')[0] }));
  return { labels: per.map((p) => p.label), c03, skipTtm: BT.qSkipTtm, skipNext: BT.skipNoNext };
});
ok('기간 라벨: 2021 4Q → 2022 1Q 부터 2023 3Q → 2023 4Q 까지 8개 (앞쪽 분기는 TTM 자료 부족으로 제외)', info.labels.length === 8 && info.labels[0] === '2021 4Q → 2022 1Q' && info.labels[7] === '2023 3Q → 2023 4Q', info.labels.join(' / '));
ok('TTM을 못 만든 종목·분기 건수가 집계됨', info.skipTtm > 0, String(info.skipTtm));
const p2 = info.c03.filter((x) => x.label === '2022 2Q → 2022 3Q')[0];
const P0 = px(3, 2022, 2), P1 = px(3, 2022, 3), epsTtm = 4 * 13e8 / 1e7;
ok('2022 2Q 보고서: 점수(EP) = TTM EPS(4분기 합) / 공시 다음 거래일 종가', p2 && p2.it && Math.abs(p2.it.v.ep - epsTtm / P0) < 1e-12, p2 && p2.it ? p2.it.v.ep + ' vs ' + epsTtm / P0 : 'none');
ok('수익률 = 다음 분기보고서(3Q) 공시 다음 거래일 종가 / 이번 종가 − 1', p2 && p2.it && Math.abs(p2.it.f - (P1 / P0 - 1)) < 1e-12, String(p2 && p2.it && p2.it.f));
const q4 = info.c03.filter((x) => x.label === '2022 4Q → 2023 1Q')[0];
ok('사업보고서(4Q) → 다음 해 1Q 공시로 이어짐, TTM = 연간값', q4 && q4.it && Math.abs(q4.it.v.ep - (4 * 13e8 / 1e7) / px(3, 2022, 4)) < 1e-12 && Math.abs(q4.it.f - (px(3, 2023, 1) / px(3, 2022, 4) - 1)) < 1e-12);
// 미래 정보 차단: 2023년 이후 보고서를 크게 바꿔도 2022 2Q 시점의 점수는 그대로여야 한다
const same = await pg.evaluate(() => {
  const pick = () => btBuildQuarterly(['ep', 'bp']).filter((p) => p.label === '2022 2Q → 2022 3Q')[0].items.map((z) => z.id + ':' + z.s.toFixed(10)).join(',');
  const a = pick();
  BT.stocks.forEach((s) => s.rows.forEach((r) => { if (Number(r.bsns_year) >= 2023) { r.net_income *= 7; r.parent_net_income *= 7; r.revenue *= 3; r.total_equity *= 2; r.parent_equity *= 2; r.ocf *= 5; } }));
  return a === pick() && a.length > 10;
});
ok('2023년 이후 재무를 바꿔도 2022 2Q 시점 점수는 불변 (미래 정보 미사용)', same);
// 실행: 결과 화면
await pg.evaluate(() => { document.getElementById('btFinMode').value = 'qttm'; btFinModeChange(); document.querySelectorAll('.btFac').forEach((c) => { c.checked = c.value === 'ep'; }); return btRunFin(); }); await pg.waitForTimeout(800);
const out = await pg.innerText('#btFinOut');
ok('결과 제목·주의사항에 분기 TTM 표기', out.includes('분기 TTM 재무') && out.includes('보유기간이 공시 간격'), out.slice(0, 80).replace(/\n/g, ' '));
ok('기간 수 T=8, 상위 그룹 종목 상세도 표시', out.includes('상위 그룹에 들어간 종목 상세') && (await pg.evaluate(() => BT_TOP.fin.res.T)) === 8);
ok('연환산은 연 4회 기준', (await pg.evaluate(() => BT_TOP.fin.res.ppy)) === 4);
// 연간 방식은 그대로
await pg.evaluate(() => { document.getElementById('btFinMode').value = 'annual'; btFinModeChange(); return btRunFin(); }); await pg.waitForTimeout(500);
ok('연간 방식도 여전히 동작(연도 라벨 FY…)', (await pg.evaluate(() => BT_TOP.fin.res.rows.map((r) => r.label))).every((l) => l.indexOf('FY') === 0));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
