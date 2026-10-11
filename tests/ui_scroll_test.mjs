// 사용법: node tests/ui_scroll_test.mjs   (Playwright + Chromium 필요. 없으면 건너뛰어도 됨)
// 종목분석 화면의 스크롤 최소화 장치(고정 내비·컴팩트 검색창·차트 접기·맨 위 버튼)를 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_ui_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href);
fs.unlinkSync(tmp);
import http from 'node:http';
import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/');
const { chromium } = require('playwright');

const posts = [];
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type','text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type','application/json');
  if (q.url.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [] }));
  if (q.url.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [] }));
  if (q.url.startsWith('/api/market/config') && q.method === 'GET') return r.end(JSON.stringify({ cfg: { minTvEok: 15, minMcapEok: 500, custom: false }, defaults: {}, protected: false }));
  if (q.url.startsWith('/api/market/config') && q.method === 'POST') { let b = ''; q.on('data', (c) => b += c); q.on('end', () => { posts.push(['cfg', JSON.parse(b)]); r.end(JSON.stringify({ ok: true })); }); return; }
  if (q.url.startsWith('/api/market/preview?mode=known')) return r.end(JSON.stringify({ codes: ['000010'] }));
  if (q.url.startsWith('/api/market/preview')) return r.end(JSON.stringify({ bas_dt: '20251010', page: 1, size: 250, total: 3, n: 3, pass: ['000010', '000020', '000030'], done: true }));
  if (q.url.startsWith('/api/market/range') && q.method === 'GET') return r.end(JSON.stringify({ job: null }));
  if (q.url.startsWith('/api/market/range') && q.method === 'POST') { let b = ''; q.on('data', (c) => b += c); q.on('end', () => { posts.push(['range', JSON.parse(b)]); r.end(JSON.stringify({ ok: true, job: { from: '20250106', to: '20250110', cur: '20250106', tradingDates: 0, skipped: 0, rows: 0, minTv: 1.5e9, minMcap: 5e10, budgetRows: 0, done: false } })); }); return; }
  if (q.url.startsWith('/api/market/funnel')) return r.end(JSON.stringify({ bas_dt: '20251010', all: 2700, tv_pass: 1300, stored: 778, min_tv_eok: 15, min_mcap_eok: 500 }));
  r.statusCode = 404; r.end('{}');
}).listen(0);
const port = srv.address().port;
const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] }).catch(async () => chromium.launch({ args: ['--no-sandbox'] }));
const pg = await b.newPage({ viewport: { width: 390, height: 800 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e))); pg.on('console', (m) => { if (m.type() === 'error' && !/404/.test(m.text())) errs.push(m.text()); });
await pg.goto('http://localhost:' + port + '/'); await pg.waitForTimeout(400);
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
// 분석 결과 영역을 강제로 보이게 하고 긴 내용을 채워 스크롤 동작을 본다
await pg.evaluate(() => { document.getElementById('stockBody').style.display = ''; const f = document.createElement('div'); f.style.height = '3000px'; f.id = 'fill'; document.getElementById('sub_summary').appendChild(f); setNavVars(); });
ok('하위 탭(요약/표/ROE/비교)이 sticky', (await pg.$eval('#stockSubnav', (e) => getComputedStyle(e).position)) === 'sticky');
await pg.evaluate(() => window.scrollTo(0, 1200)); await pg.waitForTimeout(150);
const nb = await pg.$eval('#mainNav', (e) => e.getBoundingClientRect().bottom), sb = await pg.$eval('#stockSubnav', (e) => e.getBoundingClientRect());
ok('스크롤해도 메인 탭 바로 아래에 하위 탭이 붙어 있음(겹치지 않음)', Math.abs(sb.top - nb) < 2, 'nav.bottom=' + nb + ' sub.top=' + sb.top);
ok('맨 위로 버튼이 스크롤 후 나타남', await pg.isVisible('#fabTop'));
await pg.click('#fabTop'); await pg.waitForTimeout(700);
ok('맨 위로 버튼 → 맨 위로 이동', (await pg.evaluate(() => window.scrollY)) < 5);
await pg.evaluate(() => { document.getElementById('searchCard').classList.add('compact'); document.getElementById('btnSearchMore').style.display = ''; });
ok('컴팩트 검색창: 관심종목·저장 종목·고급 설정 숨김', !(await pg.isVisible('#savedBox')) && !(await pg.isVisible('#watchlistWrap')) && await pg.isVisible('#corpName'));
await pg.click('#btnSearchMore'); ok('더보기 → 저장 종목 패널 다시 보임', await pg.isVisible('#savedBox'));
await pg.evaluate(() => { document.getElementById('searchCard').classList.add('compact'); });
await pg.focus('#corpName'); ok('검색창에 포커스하면 자동으로 펼쳐짐', await pg.isVisible('#savedBox'));
const h0 = await pg.$eval('#chartDock', (e) => e.getBoundingClientRect().height);
await pg.click('#dockFold'); const h1 = await pg.$eval('#chartDock', (e) => e.getBoundingClientRect().height);
ok('차트 접기 → 차트 영역 높이 크게 감소', h1 < h0 * 0.3, h0 + ' → ' + h1);
await pg.click('#dockFold'); ok('펼치기 → 원래대로', (await pg.$eval('#chartDock', (e) => e.getBoundingClientRect().height)) > h1 * 2);
await pg.click('#dockFold'); await pg.click('#dockBtn_growth'); ok('다른 차트 탭을 누르면 자동으로 펼쳐짐', !(await pg.$eval('#chartDock', (e) => e.classList.contains('folded'))));
ok('좁은 화면에서 주가 차트 높이 축소(250px)', await pg.evaluate(() => { document.getElementById('dockBtn_price').click(); return getComputedStyle(document.getElementById('priceCanvas')).height === '250px'; }));
await pg.evaluate(() => showFunnel()); await pg.waitForTimeout(300);
const fb = await pg.innerText('#funnelBox');
ok('유니버스 깔때기: 단계별 종목 수와 제외 수, 하루 거래대금 기준 주의 문구', fb.includes('2,700') && fb.includes('1,300') && fb.includes('778') && fb.includes('제외 1,400개') && fb.includes('제외 522개') && fb.includes('하루'), fb);
await pg.click('#tabBtnData'); await pg.waitForTimeout(400);
ok('데이터 관리: 현재 저장 기준이 입력칸에 채워짐', (await pg.inputValue('#cfgTv')) === '15' && (await pg.inputValue('#cfgMc')) === '500' && (await pg.innerText('#cfgMsg')).includes('기본값'));
await pg.fill('#cfgTv', '5'); await pg.fill('#cfgMc', '300'); await pg.click('button:has-text("이 기준이면 몇 종목")'); await pg.waitForTimeout(400);
const pv = await pg.innerText('#cfgMsg');
ok('미리보기: 통과 3개 중 신규 2개, 하루 150종목 기준 1일', pv.includes('통과 종목: 3개') && pv.includes('새로 받아야 할 종목: 2개') && pv.includes('약 1일'), pv);
await pg.click('#tabData button:has-text("저장")'); await pg.waitForTimeout(300);
ok('저장 요청 본문에 두 값 전달', posts.some((p) => p[0] === 'cfg' && p[1].min_tv_eok === '5' && p[1].min_mcap_eok === '300'));
await pg.fill('#rgFrom', '2025-01-06'); await pg.fill('#rgTo', '2025-01-10'); await pg.click('button:has-text("수집 시작")'); await pg.waitForTimeout(300);
ok('기간 수집 시작 요청과 진행 문구', posts.some((p) => p[0] === 'range' && p[1].from === '2025-01-06') && (await pg.innerText('#rgMsg')).includes('진행 중'), await pg.innerText('#rgMsg'));
ok('콘솔/페이지 오류 없음', errs.length === 0, JSON.stringify(errs));
await b.close(); srv.close(); console.log(fail ? '\n실패 ' + fail : '\n전부 통과'); process.exit(fail ? 1 : 0);
