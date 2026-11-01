// 사용법: node tests/ui_kospi_overlay_test.mjs   (Playwright + Chromium 필요)
// 가짜 API(주가 760일·재무 7개년)로 차트 독을 실제 브라우저에서 확인한다: 주가 차트, 52주 통계, 분할 의심 경고, 재무 프리셋 전환.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_kov_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/');
const { chromium } = require('playwright');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href);
fs.unlinkSync(tmp);
// 모의 데이터: 주가 760일(우상향 + 중간에 -50% 분할 의심 1회), 재무 6개년 연간 + 분기
const dates = []; { let d = new Date(Date.UTC(2023, 9, 2)); while (dates.length < 760) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0,10).replace(/-/g,'')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round((50000 + i * 40 + 3000 * Math.sin(i / 20)) * (i >= 400 ? 0.5 : 1)));
const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of [['11013','1분기',1],['11012','반기',2],['11014','3분기',3],['11011','사업보고서',4]]) {
  const g = 1 + (y - 2018) * 0.1;
  rows.push({ corp_code: 'X', bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g, net_income: 110e8 * g, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g * (po), capex: 60e8 * po, fcf: 120e8 * g * po, total_shares: 1e7, parent_net_income: 110e8 * g, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10 + y - 2018, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, dividend_per_share: 500, price_at_period_end: 50000 + (y - 2018) * 3000 });
}
// 코스피: 종목과 반대로 완만하게 하락, 처음 5일은 없음(null) → 기준일이 첫 유효일로 잡혀야 한다
const KI = dates.map((_, i) => i < 5 ? null : Math.round(2500 - i * 0.5));
const stocks = [{ n: '모의전자', c: '000001' }];
let srvNoKi = false;
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (q.url.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks }));
  if (q.url.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [{ corp_name: '모의전자', stock_code: '000001', n: rows.length, first_year: '2018', last_year: '2024', acc_mt: '12' }] }));
  if (q.url.startsWith('/api/financial-history-db')) return r.end(JSON.stringify({ corp_name: '모의전자', corp_code: 'X', acc_mt: 12, rows }));
  if (q.url.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: '모의전자', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: close.map(() => 1000), ki: srvNoKi ? null : KI, ki_src: '공공데이터포털 지수시세', ki_err: srvNoKi ? '지수 API 오류' : null }));
  if (q.url.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: '모의전자', close: close[close.length - 1], date: dates[dates.length - 1] }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'mock 404 ' + q.url }));
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
const pg = await b.newPage({ viewport: { width: 390, height: 900 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(300);
const ok = (n, v, d) => console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : ''));
await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(1500);
const green = () => pg.evaluate(() => { const c = document.getElementById('priceCanvas'); const x = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 0; i < x.length; i += 4) if (x[i + 3] > 200 && x[i] < 60 && x[i + 1] > 140 && x[i + 2] < 110) n++; return n; });
ok('겹쳐보기 체크박스 기본 켜짐', await pg.isChecked('#chkKospi'));
const g1 = await green(); ok('켜면 초록 코스피 선이 그려짐', g1 > 200, 'green px=' + g1);
ok('안내 문구에 환산·출처 설명', (await pg.innerText('#pxNote')).includes('코스피 대비 상대 성과'));
const box = await pg.locator('#priceCanvas').boundingBox();
await pg.mouse.move(box.x + box.width * 0.6, box.y + 100); await pg.waitForTimeout(150);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_kov.png') });
await pg.uncheck('#chkKospi'); await pg.waitForTimeout(100);
const g0 = await green(); ok('끄면 초록 선이 사라짐', g0 < g1 / 20, 'green px=' + g0);
await pg.check('#chkKospi'); await pg.click('#rngBtn_126'); await pg.waitForTimeout(100);
ok('6개월 범위에서도 그려짐', (await green()) > 100);
// 코스피 없는 응답
srvNoKi = true;
await pg.reload(); await pg.waitForTimeout(300); await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(1500);
ok('코스피 없어도 주가 차트 정상, 초록 선 없음', (await pg.innerText('#priceStats')).includes('52주 고가 대비') && (await green()) === 0);
ok('코스피 실패 사유를 안내', (await pg.innerText('#pxNote')).includes('코스피를 불러오지 못해'));
ok('JS 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); process.exit(0);
