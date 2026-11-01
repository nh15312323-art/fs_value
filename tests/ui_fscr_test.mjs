// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uifscr_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const SHOT = process.env.SHOT_DIR || os.tmpdir();
const dates = []; { let d = new Date(Date.UTC(2024, 9, 2)); while (dates.length < 400) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round(50000 + i * 40 + 3000 * Math.sin(i / 20)));

const mk = (code, ebitF, ocfF, niF, last) => { const rows = []; for (let y = 2018; y <= (last ? 2025 : 2024); y++) for (const [rc, lab, po] of [['11013','1분기',1],['11012','반기',2],['11014','3분기',3],['11011','사업보고서',4]]) {
  if (y === 2025 && po > 2) continue; const g = 1 + (y - 2018) * 0.1; const q = po < 4 ? 0.25 : 1;
  rows.push({ corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g * q, cogs: 600e8 * g * q, operating_income: 150e8 * g * q * ebitF, net_income: 110e8 * g * q * niF, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 45e8 * g * po * ocfF, capex: 15e8 * po, fcf: (45e8 * g * ocfF - 15e8) * po, total_shares: 1e7, treasury_shares: 0, parent_net_income: 110e8 * g * q * niF, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, price_at_period_end: 40000 * g, receivables: 150e8, inventory: 100e8, payables: 100e8, tangible_assets: 600e8 * g });
} return rows; };
const COMP = [{ corp_code: 'A', corp_name: '우량전자', stock_code: '000001', n: 1, acc_mt: 12 }, { corp_code: 'B', corp_name: '부진화학', stock_code: '000002', n: 1, acc_mt: 12 }, { corp_code: 'C', corp_name: '무시세물산', stock_code: '000003', n: 1, acc_mt: 12 }];
const DATA = { A: mk('A', 1, 1, 1, true), B: mk('B', 0.2, 0.15, 0.3, false), C: mk('C', 1, 1, 1, false) };
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0], u = new URL(q.url, 'http://x');
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [] }));
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: COMP }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => DATA[c] || [])) })); }
  if (p === '/api/fin/last-price') return r.end(JSON.stringify({ date: '20261023', prices: { '000001': 20000, '000002': 20000 } }));
  if (p === '/api/data/fresh') return r.end('{"market_last":"20261023","screen_last":null}');
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(500);
await pg.click('#bn_find'); await pg.click('#tabBtnFscr'); await pg.waitForTimeout(200);
ok('발굴 묶음에 재무 스크리닝 메뉴, 화면 열림', await pg.isVisible('#tabFscr') && await pg.isVisible('#tabBtnFscr') && (await pg.$$('#fscFilters input[type=checkbox]')).length === 12);
await pg.click('button:has-text("저장된 종목 불러오기")'); await pg.waitForTimeout(1500);
let txt = await pg.innerText('#fscOut');
ok('3개 종목 계산, 시세 3중 2개', txt.includes('재무가 저장된 종목') && txt.includes('시세가 있는 종목 2 / 3'), txt.slice(0, 200));
ok('조건 없으면 전체 3종목 표시', (await pg.$$('#fscTbl tbody tr[data-code]')).length === 3);
// 조건: FCF 마진 >= 0, PER <= 15
await pg.check('#fscOn_fcfm'); await pg.check('#fscOn_roic3'); await pg.check('#fscOn_per'); await pg.waitForTimeout(200);
txt = await pg.innerText('#fscOut');
const names = await pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.trim()));
ok('FCF 흑자 + ROIC≥10% + PER≤15 → 우량전자만', names.join() === '우량전자', names.join() + ' | ' + txt.slice(0, 400));
const fun = await pg.$$eval('#fscOut table:first-of-type tbody tr', (rs) => rs.map((r) => Array.prototype.map.call(r.children, (c) => c.textContent.trim()).join('|')));
const fl = await pg.evaluate(() => Array.prototype.map.call(document.querySelectorAll('#fscOut table')[0].querySelectorAll('tbody tr'), (r) => Array.prototype.map.call(r.children, (c) => c.textContent.trim()).join('|')));
ok('단계표: 3 → FCF 2 → ROIC 2→? → PER 1, 시세 없는 종목은 마지막에 자료 없어 제외 1', fl[0].startsWith('재무가 저장된 종목|3') && fl[1].split('|')[1] === '2' && fl[fl.length - 1].split('|')[1] === '1' && fl[fl.length - 1].split('|')[2] === '1', fl.join(' // '));
// 값 검증: 우량전자 PER = 20000*1e7 / TTM 순이익 (TTM 우선)
const v = await pg.evaluate(() => { const r = FSC.rows.filter((x) => x.name === '우량전자')[0]; return { b: r.basisTxt, per: r.per, ni: r.P.ni, fcfy: r.fcfy, fcf: r.P.fcf, mcap: r.mcap, ttm: !!r.ttm }; });
ok('TTM 우선: 기준이 TTM 2025년 2Q, PER = 시총 ÷ TTM 순이익', v.b === 'TTM 2025년 2Q' && Math.abs(v.per - v.mcap / v.ni) < 1e-9 && v.ni > 0, JSON.stringify(v));
ok('FCF 수익률 = TTM FCF ÷ 시총', Math.abs(v.fcfy - v.fcf / v.mcap) < 1e-12);
// TTM 손계산: 2025 1Q+2Q + 2024 3Q+4Q 순이익 (NI = 110e8*g*0.25, 4Q = annual - 3q)
const hand = await pg.evaluate(() => { const g24 = 1.6, g25 = 1.7; return 110e8 * g25 * 0.25 * 2 + 110e8 * g24 * 0.25 + (110e8 * g24 - 110e8 * g24 * 0.25 * 3); });
ok('TTM 순이익 손계산 일치', Math.abs(v.ni - hand) / hand < 1e-9, v.ni + ' vs ' + hand);
await pg.selectOption('#fscBasis', 'annual'); await pg.waitForTimeout(200);
ok('연간만 선택 시 기준이 2024년 연간', (await pg.evaluate(() => FSC.rows.filter((x) => x.name === '우량전자')[0].basisTxt)) === '2024년 연간');
await pg.selectOption('#fscBasis', 'ttm');
// 상세
await pg.click('#fscTbl tbody tr[data-code="A"]'); await pg.waitForTimeout(200);
const det = await pg.innerText('#fscOut');
ok('종목 클릭 → 연도별 + TTM 열의 전체 항목', det.includes('TTM(2Q)') && det.includes('영업현금흐름') && det.includes('ROIC') && det.includes('FCF 마진') && det.includes('종목분석 ›'), det.slice(det.indexOf('항목 (금액'), det.indexOf('항목 (금액') + 200));
await pg.click('button:has-text("종목분석 ›")'); await pg.waitForTimeout(300);
ok('종목분석 › → 종목분석 탭, 종목명 입력', await pg.isVisible('#tabFinancial') && (await pg.inputValue('#corpName')) === '우량전자');
// 조건 해제 후 정렬·프리셋
await pg.click('#bn_find'); await pg.click('#tabBtnFscr'); await pg.waitForTimeout(200);
await pg.click('#fscPresets button:has-text("가치")'); await pg.waitForTimeout(200);
const on = await pg.$$eval('#fscFilters input[type=checkbox]', (e) => e.filter((x) => x.checked).map((x) => x.id));
ok('프리셋 가치: FCF수익률·PER·ROIC 켜짐', on.sort().join() === ['fscOn_fcfy', 'fscOn_per', 'fscOn_roic3'].sort().join(), on.join());
await pg.click('#fscPresets button:has-text("전부 해제")'); await pg.click('#fscTbl th:has-text("PER")'); await pg.waitForTimeout(200);
const order = await pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.trim()));
ok('PER 오름차순 정렬, N/A는 맨 아래', order[order.length - 1] === '무시세물산' && order[0] === '우량전자', order.join());
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
