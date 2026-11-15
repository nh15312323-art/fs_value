// 사용법: node tests/ui_fincov_test.mjs (Playwright + Chromium 필요)
// 재무 스크리닝·재무 표·종목분석에서 (1) 종가·시가총액이 나오는지 (2) 시장 DB에 없는 종목은 실시간 시세로 채우는지
// (3) DART 주식수가 비어 있어도(금융·지주) 시가총액 기준 PER·PBR이 계산되는지 (4) 빈 곳은 N/A + 자료 점검에 사유가 남는지 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uicov_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const PERIODS = [['11013', '1분기', 1], ['11012', '반기', 2], ['11014', '3분기', 3], ['11011', '사업보고서', 4]];
// k: 규모 배율, noShares: DART 주식수 없음(금융·지주에서 흔한 상황을 흉내)
const rowsOf = (code, k, noShares, bank) => { const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of PERIODS) {
  const g = 1 + (y - 2018) * 0.1, q = po / 4;
  rows.push({ corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS',
    revenue: bank ? null : 1000e8 * g * q * k, cogs: bank ? null : 600e8 * g * q * k, operating_income: bank ? null : 150e8 * g * q * k,
    net_income: 100e8 * g * q * k, parent_net_income: 100e8 * g * q * k, total_equity: 1000e8 * g * k, parent_equity: 1000e8 * g * k, total_liabilities: 500e8 * g * k,
    ocf: 140e8 * g * q * k, capex: 40e8 * g * q * k, fcf: 100e8 * g * q * k, total_shares: noShares ? null : 1e7, treasury_shares: noShares ? null : 0,
    current_assets: bank ? null : 400e8 * g, current_liabilities: bank ? null : 250e8 * g, filing_date: (y + (rc === '11011' ? 1 : 0)) + '0320', price_at_period_end: 20000 }); } return rows; };
const COMP = [
  { corp_code: 'G', corp_name: '모의전자', stock_code: '000001', ind: '26110', n: 28, acc_mt: 12 },   // 시장 DB에 시세·시총 있음, DART 주식수 있음
  { corp_code: 'B', corp_name: '모의은행', stock_code: '000002', ind: '64121', n: 28, acc_mt: 12 },   // 시장 DB에 있음, DART 주식수 없음
  { corp_code: 'H', corp_name: '모의지주', stock_code: '000003', ind: '64992', n: 28, acc_mt: 12 },   // 시장 DB에 없음 → 실시간 조회로 채움, DART 주식수 없음
  { corp_code: 'I', corp_name: '모의생명', stock_code: '000004', ind: '65110', n: 28, acc_mt: 12 },   // 시장 DB에 없음 → 실시간 조회로 채움
  { corp_code: 'F', corp_name: '모의투자', stock_code: '000005', ind: '64999', n: 28, acc_mt: 12 },   // 어디에도 시세 없음
];
const DATA = { G: rowsOf('G', 1, false, false), B: rowsOf('B', 4, true, true), H: rowsOf('H', 2, true, false), I: rowsOf('I', 3, false, true), F: rowsOf('F', 1, false, false) };
const byName = {}; COMP.forEach((c) => { byName[c.corp_name] = c; });
const asked = { quotes: [], latest: [] };
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: COMP.map((c) => ({ n: c.corp_name, c: c.stock_code })) }));
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: COMP }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => DATA[c] || [])) })); }
  if (p === '/api/fin/last-price') return r.end(JSON.stringify({ date: '20261113', prices: { '000001': 20000, '000002': 50000 }, caps: { '000001': { cap: 2000e8, close: 20000 }, '000002': { cap: 8000e8, close: 50000 } } }));
  if (p === '/api/fin/quotes') { const cs = (u.searchParams.get('codes') || '').split(','); asked.quotes.push(cs.length);
    const quotes = {}, errs = {};
    cs.forEach((c) => { if (c === '000003') quotes[c] = ['20261113', 30000, 3000e8, 1000, 3e9]; else if (c === '000004') quotes[c] = ['20261112', 40000, 6000e8, 500, 2e9]; else errs[c] = '최근 10일 시세 없음(모의)'; });
    return r.end(JSON.stringify({ quotes, errs })); }
  if (p === '/api/fin/market-snap') return r.end(JSON.stringify({ date: '20261113', n: 2, rows: { '000001': ['20261113', 20000, 25000, 15000, 250, 1000, 2e9, 2e9, 2000e8, 0], '000002': ['20261113', 50000, 60000, 40000, 250, 1000, 2e9, 2e9, 8000e8, 0] } }));
  if (p === '/api/market/caps') return r.end(JSON.stringify({ date: '20261113', caps: {} }));
  if (p.startsWith('/api/price/series')) { r.statusCode = 404; return r.end('{"error":"시세 없음(모의)"}'); }
  if (p.startsWith('/api/latest-price')) { const nm = u.searchParams.get('corp_name'); asked.latest.push(nm);
    if (nm === '모의은행') return r.end(JSON.stringify({ corp_name: nm, close: 50000, date: '20261113', mcap: 8000e8, mcap_close: 50000, mcap_date: '20261113', mcap_src: '시장 DB' }));
    if (nm === '모의전자') return r.end(JSON.stringify({ corp_name: nm, close: 20000, date: '20261113', mcap: null, mcap_close: null, mcap_date: null, mcap_src: null }));
    return r.end(JSON.stringify({ corp_name: nm, close: 30000, date: '20261113', mcap: 3000e8, mcap_close: 30000, mcap_date: '20261113', mcap_src: '공공데이터포털' })); }
  if (p.startsWith('/api/financial-history-db')) { const c = byName[u.searchParams.get('corp_name')]; if (!c) { r.statusCode = 404; return r.end('{"error":"none"}'); } return r.end(JSON.stringify({ corp_name: c.corp_name, stock_code: c.stock_code, induty_code: c.ind, acc_mt: 12, rows: DATA[c.corp_code] })); }
  if (p.startsWith('/api/industry/')) return r.end('{"map":{}}');
  r.statusCode = 404; r.end('{"error":"none"}');
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(500);

// ---- 재무 스크리닝
await pg.evaluate(() => switchTab('fscr')); await pg.click('button[onclick="fscLoad()"]'); await pg.waitForFunction(() => FSC.rows && FSC.rows.length === 5 && !FSC.loading, null, { timeout: 15000 });
const R = await pg.evaluate(() => { const o = {}; FSC.rows.forEach((r) => { o[r.stock] = { price: r.price, mcap: r.mcap, per: r.per, pbr: r.pbr, src: r.mcapSrc, chk: r.chk }; }); return o; });
const near = (a, b) => a != null && Math.abs(a / b - 1) < 1e-6;
ok('시장 DB 종목(모의전자): 종가 20,000, 시가총액 2000억', R['000001'].price === 20000 && near(R['000001'].mcap, 2000e8), JSON.stringify(R['000001']));
ok('시장 DB 종목(모의은행)·DART 주식수 없음: 시총 8000억 → PER·PBR 계산됨', near(R['000002'].mcap, 8000e8) && R['000002'].per > 0 && R['000002'].pbr > 0, JSON.stringify(R['000002']));
// 은행: 순이익 TTM(2024 연간)=100e8*1.6*4*… 사업보고서 값 기준으로 손계산(k=4, g=1.6, q=1): ni=100e8*1.6*1*4=640e8, equity=1000e8*1.6*4=6400e8
ok('손계산: 은행 PER = 8000억 ÷ 640억 = 12.5, PBR = 8000억 ÷ 6400억 = 1.25', Math.abs(R['000002'].per - 12.5) < 1e-6 && Math.abs(R['000002'].pbr - 1.25) < 1e-6, R['000002'].per + ' / ' + R['000002'].pbr);
ok('시장 DB에 없는 지주(모의지주): 실시간 시세로 종가 30,000, 시총 3000억, DART 주식수 없어도 PER·PBR', R['000003'].price === 30000 && near(R['000003'].mcap, 3000e8) && R['000003'].per > 0 && R['000003'].pbr > 0 && /실시간/.test(R['000003'].src), JSON.stringify(R['000003']));
ok('시장 DB에 없는 생명(모의생명): 실시간 시세로 종가 40,000, 시총 6000억', R['000004'].price === 40000 && near(R['000004'].mcap, 6000e8), JSON.stringify(R['000004']));
ok('어디에도 시세 없는 종목: 종가·시총·PER·PBR 모두 N/A(만들지 않음)', R['000005'].price === null && R['000005'].mcap === null && R['000005'].per === null && R['000005'].pbr === null);
ok('실시간 조회 요청은 6개 이하씩(서브리퀘스트 한도), 시장 DB에 있는 종목은 조회하지 않음', asked.quotes.length >= 1 && asked.quotes.every((n) => n <= 6) && asked.quotes.reduce((a, n) => a + n, 0) === 3, JSON.stringify(asked.quotes));
const ths = await pg.$$eval('#fscTbl thead th', (e) => e.map((x) => x.textContent.replace(/[▲▼ ]/g, '')));
ok('표에 종가·시가총액 열이 있음', ths.includes('종가') && ths.includes('시가총액'), ths.join('|'));
const rowTxt = await pg.$$eval('#fscTbl tbody tr[data-code]', (e) => e.map((x) => x.innerText.replace(/\s+/g, ' ')));
ok('표에 시가총액이 억/조 단위로 표시', rowTxt.some((t) => /3,000억/.test(t)) && rowTxt.some((t) => /8,000억/.test(t)), rowTxt.join(' || ').slice(0, 300));
const audit = await pg.$eval('#fscOut details', (e) => e.textContent);
ok('자료 점검: 시세 없는 종목(모의투자)과 사유가 표시됨', /모의투자/.test(audit) && /최근 10일 시세 없음\(모의\)/.test(audit), audit.slice(0, 200));
ok('자료 점검: DART 주식수 없음 집계(은행·지주 등 유형별)', /DART 주식수 없음/.test(audit));
await pg.click('#fscTbl th >> text=시가총액'); await pg.waitForTimeout(100);
const order = await pg.$$eval('#fscTbl tbody tr[data-code]', (e) => e.map((x) => x.getAttribute('data-code')));
ok('시가총액 열 정렬(내림차순): 8000억 → 6000억 → 3000억 → 2000억 → N/A는 맨 아래', order.join() === 'B,I,H,G,F', order.join());

// ---- 재무 표
await pg.evaluate(() => switchTab('ft')); await pg.click('button[onclick="ftLoad(false)"]'); await pg.waitForFunction(() => FT.rows && FT.rows.length === 5 && !FT.loading, null, { timeout: 15000 });
const T = await pg.evaluate(() => { const o = {}; FT.rows.forEach((r) => { o[r.stock] = { price: r.price, mcap: r.mcap, perFY: r.perFY, pbrFY: r.pbrFY, fromHi: r.fromHi, note: r.hiNote }; }); return o; });
ok('재무 표: 시장 DB에 없는 지주도 종가·시총·PER·PBR 표시, 52주 값은 N/A + 사유', T['000003'].price === 30000 && near(T['000003'].mcap, 3000e8) && T['000003'].perFY > 0 && T['000003'].pbrFY > 0 && T['000003'].fromHi === null && /52주 값 없음/.test(T['000003'].note), JSON.stringify(T['000003']));
ok('재무 표: 시세 없는 종목은 N/A', T['000005'].price === null && T['000005'].perFY === null);
ok('재무 표: 시장 DB 종목은 52주 값 유지(모의전자 −20%)', Math.abs(T['000001'].fromHi - (20000 / 25000 - 1)) < 1e-9, T['000001'].fromHi);

// ---- 종목분석: DART 주식수가 없는 은행·지주도 시총·PER·PBR
async function analyze(name) {
  await pg.evaluate(() => switchTab('financial')); await pg.fill('#corpName', name); await pg.evaluate(() => analyzeStock());
  await pg.waitForFunction(() => lastSummaryMetrics && lastSummaryMetrics.priceInput > 0, null, { timeout: 15000 }); await pg.waitForTimeout(300);
  return pg.evaluate(() => ({ m: { per: lastSummaryMetrics.per, pbr: lastSummaryMetrics.pbr, cap: lastSummaryMetrics.marketCap, est: lastSummaryMetrics.sharesEst, eps: lastSummaryMetrics.eps, bps: lastSummaryMetrics.bps }, note: document.getElementById('priceNote').textContent }));
}
let A = await analyze('모의은행');
ok('종목분석(모의은행·DART 주식수 없음): 시총 8000억, PER 12.5, PBR 1.25, 주식수는 추정 표시', near(A.m.cap, 8000e8) && Math.abs(A.m.per - 12.5) < 1e-6 && Math.abs(A.m.pbr - 1.25) < 1e-6 && A.m.est === true, JSON.stringify(A.m));
ok('가격 안내문에 시가총액과 출처', /8,000억\(시장 DB/.test(A.note), A.note);
A = await analyze('모의전자');
ok('종목분석(모의전자·시가총액 못 받음·DART 주식수 있음): 기존대로 종가×주식수(시총 2000억), 추정 아님', near(A.m.cap, 2000e8) && A.m.est === false && A.m.per > 0, JSON.stringify(A.m));
ok('가격 안내문: 시장 시총을 못 받았다고 알림', /시장 시가총액은 확인하지 못해/.test(A.note), A.note);
await pg.evaluate(() => { document.getElementById('currentPrice').value = '40000'; renderSummary(); });
const A2 = await pg.evaluate(() => ({ cap: lastSummaryMetrics.marketCap }));
ok('현재가를 직접 40,000원으로 바꾸면 시총도 비례해 4000억', near(A2.cap, 4000e8), A2.cap);
A = await analyze('모의지주');
ok('종목분석(모의지주): 현재가 30,000원 기준 시총 3000억·PER·PBR 계산(주식수 추정)', near(A.m.cap, 3000e8) && A.m.per > 0 && A.m.pbr > 0 && A.m.est === true, JSON.stringify(A.m));
ok('JS 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
