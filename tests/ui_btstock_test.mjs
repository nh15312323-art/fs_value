// 사용법: node tests/ui_btstock_test.mjs (Playwright + Chromium 필요)
// ⑤ 백테스트 C. 종목별 보기: 종목을 고르면 재무 값 · 매수·매도가 · 12개월 수익률 · 팩터 값이 한 표에 나오는지 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uibtstk_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const mk = (code, y, g) => ({ corp_code: code, bsns_year: String(y), reprt_code: '11011', period_label: y + ' 사업보고서', period_order: y * 10 + 4, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g, net_income: 110e8 * g, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g, capex: 60e8, fcf: 120e8 * g, total_shares: 1e7, treasury_shares: 0, parent_net_income: 110e8 * g, parent_equity: 2000e8 * g, filing_date: (y + 1) + '0320', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, dividend_per_share: 500, price_at_period_end: 40000 * g, price_next_day: 41000 * g, price_next_day_date: (y + 1) + '0321' });
const rowsA = []; for (let y = 2018; y <= 2024; y++) rowsA.push(mk('A', y, 1 + (y - 2018) * 0.1));
const rowsB = []; for (let y = 2022; y <= 2024; y++) rowsB.push(mk('B', y, 1));
const fetched = [];
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: [{ corp_code: 'A', corp_name: '가나전자', stock_code: '000010', n: 28, acc_mt: 12 }, { corp_code: 'B', corp_name: '다라화학', stock_code: '000020', n: 12, acc_mt: 3 }] }));
  if (p === '/api/backtest/fin-batch') { fetched.push(u.searchParams.get('codes')); return r.end(JSON.stringify({ rows: u.searchParams.get('codes') === 'A' ? rowsA : rowsB })); }
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
ok('C 카드와 종목 입력칸 표시', (await pg.innerText('#tabBt')).includes('C. 종목별 보기') && !!(await pg.$('#btStkIn')));
ok('탭을 열면 저장된 종목 목록(2개)이 자동완성에 채워짐', (await pg.$$('#btStkList option')).length === 2);
await pg.fill('#btStkIn', '가나'); await pg.press('#btStkIn', 'Enter'); await pg.waitForTimeout(700);
const out = await pg.innerText('#btStkOut');
ok('앞부분만 입력해도 종목을 찾고 한 표로 표시(가나전자, 7개 연도)', out.includes('가나전자') && out.includes('결산연도 7개'), out.slice(0, 80).replace(/\n/g, ' '));
ok('열 머리글: 매수가·매도가·12개월 수익률·ROIC·F-Score·FCF수익률', ['매수가', '매도가', '12개월', 'ROIC', 'F-Score', 'FCF수익률', 'PER×PBR'].every((t) => out.includes(t)));
const first = await pg.evaluate(() => { const tr = document.querySelectorAll('#btStkOut tbody tr'); return { n: tr.length, y0: tr[0].children[0].innerText, y6: tr[6].children[0].innerText, ret6: tr[6].children[5].innerText, buy6: tr[6].children[3].innerText, sell6: tr[6].children[4].innerText, last: tr[0].children[5].innerText, st0: tr[0].lastElementChild.innerText }; });
ok('7행, 최신(2024)이 위·2018이 아래', first.n === 7 && first.y0 === '2024' && first.y6 === '2018', JSON.stringify(first));
ok('2018: 매수 41,000 → 매도 45,100(2019년분) = +10.0%', first.buy6 === '41,000' && first.sell6 === '45,100' && first.ret6 === '+10.0%', first.buy6 + ' ' + first.sell6 + ' ' + first.ret6);
ok('2024는 다음 해 공시 전: 수익률 N/A + 「보유 중」', first.last === 'N/A' && first.st0.includes('보유 중'), first.st0);
ok('서버에는 이 종목 하나만 요청(전체 불러오기 없이)', fetched.length === 1 && fetched[0] === 'A');
ok('모바일 가로 넘침 없음(표는 칸 안에서 스크롤)', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
await pg.evaluate(() => { window.__dlName = null; const o = HTMLAnchorElement.prototype.click; HTMLAnchorElement.prototype.click = function () { window.__dlName = this.download; return o.apply(this, arguments); }; });
const [dl] = await Promise.all([pg.waitForEvent('download'), pg.click('button:has-text("이 종목 CSV")')]);
const csvText = fs.readFileSync(await dl.path(), 'utf8');
const dlName = await pg.evaluate(() => window.__dlName);
ok('CSV 내려받기: 파일명에 종목명, 머리글+7행, 엑셀용 BOM', dlName === 'backtest_가나전자.csv' && csvText.charCodeAt(0) === 0xFEFF && csvText.replace(/^\uFEFF/, '').split('\n').length === 8, dlName);
await pg.click('button:has-text("전체 종목 CSV")'); await pg.waitForTimeout(150);
ok('전체 CSV는 ① 불러오기 전에는 안내만', (await pg.innerText('#btStkMsg')).includes('① 저장된 종목 불러오기'));
await pg.fill('#btStkIn', '다라화학'); await pg.press('#btStkIn', 'Enter'); await pg.waitForTimeout(700);
const out2 = await pg.innerText('#btStkOut');
ok('12월 결산이 아닌 종목은 경고 배너 + 표', out2.includes('결산월이 3월') && out2.includes('다라화학') && out2.includes('결산연도 3개'));
await pg.fill('#btStkIn', '없는종목'); await pg.press('#btStkIn', 'Enter'); await pg.waitForTimeout(300);
ok('없는 종목은 안내 문구', (await pg.innerText('#btStkMsg')).includes('찾지 못했습니다'));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
