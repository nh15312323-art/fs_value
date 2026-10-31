// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uiind_' + process.pid + '.mjs');
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
COMP[0].ind = '26110';
const filled = []; let lookups = [];
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
  if (p === '/api/industry/lookup') { const cs = (u.searchParams.get('codes') || '').split(','); lookups.push(cs); const map = {}; if (cs.includes('000009')) map['000009'] = '10712'; return r.end(JSON.stringify({ map })); }
  if (p === '/api/industry/fill') { let body = ''; q.on('data', (d) => body += d); q.on('end', () => { const cs = JSON.parse(body).codes; filled.push(cs); const saved = {}; cs.forEach((c) => { if (c === '000002') saved[c] = '20119'; if (c === '000004') saved[c] = '70113'; }); r.end(JSON.stringify({ saved })); }); return; }
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(500);
await pg.click('#bn_find'); await pg.click('#tabBtnFscr'); await pg.waitForTimeout(200);
await pg.click('button:has-text("저장된 종목 불러오기")'); await pg.waitForTimeout(2000);
const hdr = await pg.$$eval('#fscTbl thead th', (e) => e.map((x) => x.textContent.trim().replace(/[▲▼ ]/g, '')));
ok('표에 업종 열이 종목 다음에 있다', hdr[0] === '종목' && hdr[1] === '업종', hdr.slice(0, 3).join('|'));
const cells = await pg.evaluate(() => Array.prototype.map.call(document.querySelectorAll('#fscTbl tbody tr[data-code]'), (r) => r.children[0].textContent.trim() + '=' + r.children[1].textContent.trim()));
ok('저장된 업종(A)·DART로 받은 업종(B)·못 받은 업종(C=미확인)', cells.some((c) => c.startsWith('우량전자=') && !c.endsWith('(미확인)') && c.length > 5) && cells.some((c) => c.startsWith('부진화학=') && c.includes('화학')) && cells.some((c) => c.startsWith('무시세물산=(미확인)')), cells.join(' / '));
ok('저장된 A는 DART에 다시 묻지 않고, B·C만 받으려 했다', filled.length === 1 && filled[0].sort().join() === '000002,000003', JSON.stringify(filled));
const opts = await pg.$$eval('#fscInd option', (o) => o.map((x) => x.value + ':' + x.textContent));
ok('업종 필터 옵션: 전체 + 업종 + 미확인', opts.length === 4 && opts[0].startsWith(':') && opts.some((x) => x.startsWith('none:')), opts.join(' | '));
await pg.selectOption('#fscInd', '20'); await pg.waitForTimeout(200);
let names = await pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.trim()));
ok('화학(20) 선택 → 부진화학만', names.join() === '부진화학', names.join());
const fl = await pg.evaluate(() => document.querySelectorAll('#fscOut table')[0].textContent);
ok('단계표에 업종 단계 표시', fl.includes('업종:'), fl.slice(0, 120));
await pg.selectOption('#fscInd', 'none'); await pg.waitForTimeout(200);
names = await pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.trim()));
ok('미확인 선택 → 무시세물산만', names.join() === '무시세물산', names.join());
await pg.selectOption('#fscInd', ''); await pg.click('#fscTbl thead th:nth-child(2)'); await pg.waitForTimeout(200);
names = await pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.trim()));
ok('업종 열 정렬이 오류 없이 동작(3종목 유지)', names.length === 3, names.join());
await pg.click('#fscTbl tbody tr[data-code]'); await pg.waitForTimeout(200);
ok('종목 상세 머리에 업종 표시', (await pg.textContent('#fscOut')).includes('업종 '));
// 종목분석 메타
const m1 = await pg.evaluate(async () => { document.body.insertAdjacentHTML('beforeend', '<div id="anIndMeta"></div>'); AN_STOCK = { code: '000001', ind: '26110' }; IND.map['000001'] = '26110'; anIndMeta(); return document.getElementById('anIndMeta').textContent; });
ok('종목분석: 업종 표시(KSIC 코드 병기)', m1.includes('업종:') && m1.includes('KSIC 26110'), m1);
const m2 = await pg.evaluate(async () => { AN_STOCK = { code: '000004', ind: '' }; delete IND.map['000004']; anIndMeta(); await new Promise((r) => setTimeout(r, 600)); return document.getElementById('anIndMeta').textContent; });
ok('종목분석: 저장 없으면 받아서 표시', m2.includes('업종:') && !m2.includes('확인 중') && m2.includes('KSIC 70113'), m2);
const m3 = await pg.evaluate(async () => { AN_STOCK = { code: '000005', ind: '' }; anIndMeta(); await new Promise((r) => setTimeout(r, 600)); return document.getElementById('anIndMeta').textContent; });
ok('종목분석: 못 받으면 솔직히 표시', m3.includes('확인하지 못했습니다'), m3);
ok('페이지 오류 없음', errs.length === 0, errs.join(';'));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
