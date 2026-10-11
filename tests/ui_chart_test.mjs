// 사용법: node tests/ui_chart_test.mjs   (Playwright + Chromium 필요)
// 가짜 API(주가 760일·재무 7개년)로 차트 독을 실제 브라우저에서 확인한다: 주가 차트, 52주 통계, 분할 의심 경고, 재무 프리셋 전환.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_chart_' + process.pid + '.mjs');
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
const stocks = [{ n: '모의전자', c: '000001' }];
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (q.url.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks }));
  if (q.url.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [{ corp_name: '모의전자', stock_code: '000001', n: rows.length, first_year: '2018', last_year: '2024', acc_mt: '12' }] }));
  if (q.url.startsWith('/api/financial-history-db')) return r.end(JSON.stringify({ corp_name: '모의전자', corp_code: 'X', acc_mt: 12, rows }));
  if (q.url.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: '모의전자', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: close.map(() => 1000) }));
  if (q.url.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: '모의전자', close: close[close.length - 1], date: dates[dates.length - 1] }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'mock 404 ' + q.url }));
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
const pg = await b.newPage({ viewport: { width: 390, height: 900 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(300);
const ok = (n, v, d) => console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : ''));
await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(1500);
ok('분석 후 차트 독 표시', await pg.isVisible('#chartDock'));
const stats = await pg.innerText('#priceStats');
ok('주가 통계(52주 고가·저가·이동평균 대비) 표시', stats.includes('52주 고가 대비') && stats.includes('200일선 대비') && stats.includes('최대낙폭'), stats.slice(0, 160));
const note = await pg.innerText('#pxNote');
ok('±30% 초과 변동(분할 의심) 경고 문구', note.includes('하루 ±30% 초과 변동 1회'), note.slice(0, 120));
const painted = await pg.evaluate(() => { const c = document.getElementById('priceCanvas'); const x = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 3; i < x.length; i += 4) if (x[i] > 0) n++; return n; });
ok('주가 캔버스에 실제로 그려짐', painted > 2000, 'painted px=' + painted);
const box = await pg.locator('#priceCanvas').boundingBox();
await pg.mouse.move(box.x + box.width * 0.6, box.y + 100); await pg.waitForTimeout(150);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_chart_price.png') });
await pg.click('#rngBtn_126'); await pg.waitForTimeout(100);
ok('6개월 버튼 활성', (await pg.getAttribute('#rngBtn_126', 'class')) === 'toggle-active');
await pg.click('#dockBtn_growth'); await pg.waitForTimeout(300);
ok('성장 프리셋: 재무 차트 표시, 주가 숨김', await pg.isVisible('#chartWrap') && !(await pg.isVisible('#priceBox')));
const chips = await pg.innerText('#chartChips');
ok('프리셋 칩: 매출액·영업이익·영업이익률', chips.includes('매출액') && chips.includes('영업이익률'), chips.replace(/\n/g, ' ').slice(0, 80));
await pg.click('#dockA'); await pg.waitForTimeout(300);
ok('연간 전환 버튼 동작', (await pg.getAttribute('#dockA', 'class')) === 'toggle-active' && await pg.isVisible('#chartWrap'));
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_chart_growth.png') });
await pg.click('#dockBtn_value'); await pg.waitForTimeout(200);
ok('밸류에이션 프리셋: PER·PBR·FCF Yield', (await pg.innerText('#chartChips')).includes('PER'));
await pg.click('#subBtn_table'); await pg.waitForTimeout(150);
ok('다른 하위 탭에서도 차트 독 유지', await pg.isVisible('#chartDock'));
await pg.click('#dockBtn_price'); await pg.waitForTimeout(200);
ok('주가로 복귀', await pg.isVisible('#priceBox') && !(await pg.isVisible('#chartWrap')));
ok('자바스크립트 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
