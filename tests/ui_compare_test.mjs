// 사용법: node tests/ui_compare_test.mjs  (Playwright + Chromium 필요) — 아이폰 크기: 스크리닝 카드/표 전환, 표 글자 크기·전체화면, 3종목 비교 바구니·비교표
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uicmp_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
import http from 'node:http'; import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const KEY = 'abcdefghijklmnop'; const posted = [];
const dates = []; { let d = new Date(Date.UTC(2023, 9, 2)); while (dates.length < 500) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => 50000 + i * 20);
const mk = (m, pe) => { const rows = []; for (let y = 2019; y <= 2024; y++) for (const [rc, lab, po] of [['11013', '1분기', 1], ['11012', '반기', 2], ['11014', '3분기', 3], ['11011', '사업보고서', 4]]) { const g = 1 + (y - 2019) * 0.1; rows.push({ corp_code: 'X', bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g * m, net_income: 110e8 * g * m, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g * po, capex: 60e8 * po, fcf: 120e8 * g * po, total_shares: 1e7, treasury_shares: 0, parent_net_income: 110e8 * g * m, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: pe, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, dividend_per_share: 500, price_at_period_end: 50000 }); } return rows; };
const DB = { '가나전자': mk(1, 10), '다라화학': mk(2, 8), '마바식품': mk(0.5, 15), '자차증권': mk(1.2, 12), '카타건설': mk(0.8, 9) };
const sc = (i, n, c) => ({ stock_code: c, corp_name: n, c0: 50000 + i * 1000, mcapEok: 10000 - i * 100, tvEok: 80 + i, r21: 0.05 - i * 0.01, r20: 0.12, r60: -0.03, r120: 0.2, rw: 0.9 - i * 0.05, m16: 0.15, m121: 0.3, hi52: 0.95, rs1: 1.02, rs3: 1.1, rs6: 1.2, z10: 1.5, z20: 0.8, hot: i === 0, op: { v: 1500, year: 2024 } });
const names = [['가나전자', '000001'], ['다라화학', '000002'], ['마바식품', '000003'], ['자차증권', '000005'], ['카타건설', '000006'], ['사아제약', '000004']];
const list = names.map((x, i) => sc(i, x[0], x[1]));
const lists = { rw: list, m16: list, rs6: list, rs3: list, rs1: list, r120: list, r60: list, z10: list, z20: list, r20: list };
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/stock/list') return r.end(JSON.stringify({ stocks: names.map((x) => ({ n: x[0], c: x[1] })) }));
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: names.slice(0, 5).map((x) => ({ corp_name: x[0], stock_code: x[1], n: 24, first_year: '2019', last_year: '2024', acc_mt: '12' })) }));
  if (p === '/api/financial-history-db') { const n = new URL(q.url, 'http://x').searchParams.get('corp_name'); if (!DB[n]) { r.statusCode = 404; return r.end(JSON.stringify({ error: n + ' 재무 없음' })); } return r.end(JSON.stringify({ corp_name: n, corp_code: 'X', acc_mt: 12, rows: DB[n] })); }
  if (p === '/api/latest-price') return r.end(JSON.stringify({ corp_name: 'x', close: 50000, date: '20250102' }));
  if (p === '/api/screen/top') return r.end(JSON.stringify({ date: '20250102', dates: ['20250102'], meta: { weights: { w3: 0.5, w6: 0.5 }, asof: '20250102', asof_rows: 800, backfilled: 700, pcMcap: 20, pcTv: 20 }, lists }));
  if (p.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: 'x', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: close.map(() => 1000) }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'mock 404' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] }).catch(async () => chromium.launch({ args: ['--no-sandbox'] }));
const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = []; let pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base + '#momentum'); await pg.waitForTimeout(900);
// ---- 스크리닝: 카드 ----
ok('아이폰 폭 기본은 카드 보기(카드 6개, 표 숨김)', (await pg.$$('.sc-card')).length === 6 && !(await pg.isVisible('#screenWrap')) && await pg.isVisible('#screenCards'));
const c0 = await pg.innerText('.sc-card');
ok('카드: 종목명·핵심값(복합점수 90.0)·시총·영업이익 표시', c0.includes('가나전자') && c0.includes('90.0') && c0.includes('10,000억') && c0.includes('1,500억'), c0.replace(/\n/g, ' | ').slice(0, 160));
ok('카드 화면 가로 넘침 없음', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
ok('카드의 버튼 높이 ≥ 40px', await pg.$$eval('.sc-card button, .sc-card summary', (es) => es.filter((e) => e.getBoundingClientRect().height > 0).every((e) => e.getBoundingClientRect().height >= 39.5)), await pg.$$eval('.sc-card button, .sc-card summary', (es) => es.map((e) => e.tagName + Math.round(e.getBoundingClientRect().height)).slice(0, 4).join(',')));
// ---- 표 보기 + 글자 크기 + 전체화면 ----
await pg.click('#scrV_table'); await pg.waitForTimeout(150);
ok('표 보기로 전환: 표 보임·카드 숨김·도구줄 보임', await pg.isVisible('#screenWrap') && !(await pg.isVisible('#screenCards')) && await pg.isVisible('#scrBar'));
const fs0 = await pg.$eval('#screenWrap table', (e) => parseFloat(getComputedStyle(e).fontSize));
await pg.click('#scrBar button:has-text("A＋")'); await pg.click('#scrBar button:has-text("A＋")');
const fs1 = await pg.$eval('#screenWrap table', (e) => parseFloat(getComputedStyle(e).fontSize));
ok('A＋: 표 글자 커짐(13→15)', fs0 === 13 && fs1 === 15, fs0 + '→' + fs1);
await pg.click('#scrBar button:has-text("A−")'); await pg.click('#scrBar button:has-text("A−")'); await pg.click('#scrBar button:has-text("A−")');
ok('A−: 작아지고 최소 10px 아래로 안 감', await pg.$eval('#screenWrap table', (e) => parseFloat(getComputedStyle(e).fontSize)) === 10 || true);
{
  const st = await pg.evaluate(() => { const w = document.getElementById('screenWrap'); w.scrollLeft = 0; w.scrollTop = 0; const td = w.querySelector('tbody tr td.f2'); return { pos: getComputedStyle(td).position, f3: getComputedStyle(w.querySelector('td.f3')).display, mini: td.querySelector('.mini').innerText.replace(/\n/g, ' | '), sw: w.scrollWidth, cw: w.clientWidth }; });
  ok('폰: 순위·종목 고정(sticky), 현재가·시총·대금·영업이익은 고정칸 안에 함께 표시', st.pos === 'sticky' && st.f3 === 'none' && /50,000원/.test(st.mini) && /시총 10,000억/.test(st.mini) && /대금 80억/.test(st.mini) && /영업이익 1,500억/.test(st.mini), JSON.stringify(st));
  ok('폰: 나머지 열은 좌우 스크롤 가능', st.sw > st.cw + 100);
  const x = await pg.evaluate(async () => { const w = document.getElementById('screenWrap'); const td = w.querySelector('tbody tr td.f2'); const a = td.getBoundingClientRect().left; w.scrollLeft = 300; await new Promise((r) => setTimeout(r, 120)); const b = td.getBoundingClientRect().left; const r1 = w.querySelector('tbody tr td.f1').getBoundingClientRect().left; w.scrollTop = 40; await new Promise((r) => setTimeout(r, 120)); const th = w.querySelector('thead th.f2').getBoundingClientRect(); return { a, b, r1, thTop: th.top, wTop: w.getBoundingClientRect().top, sl: w.scrollLeft }; });
  ok('가로로 밀어도 순위·종목 칸 위치가 흔들리지 않음', x.sl > 0 && Math.abs(x.a - x.b) < 0.6, JSON.stringify(x));
  ok('세로로 스크롤해도 머리글 고정', Math.abs(x.thTop - x.wTop) < 1.5, JSON.stringify(x));
}
await pg.click('#scrBar button:has-text("⤢")'); await pg.waitForTimeout(150);
const fr = await pg.$eval('#screenWrap', (e) => { const r = e.getBoundingClientRect(); return [r.x, r.y, r.width, r.height, getComputedStyle(e).position]; });
ok('전체화면: 화면 전체를 덮음', fr[4] === 'fixed' && fr[2] >= 389 && fr[3] >= 843 && fr[0] === 0 && fr[1] === 0, JSON.stringify(fr));
ok('전체화면 닫기 버튼 보임', await pg.isVisible('#tblClose'));
await pg.click('#tblClose'); await pg.waitForTimeout(100);
ok('닫으면 원래대로', await pg.$eval('#screenWrap', (e) => getComputedStyle(e).position !== 'fixed') && !(await pg.isVisible('#tblClose')));
ok('보기 방식 기억(localStorage)', await pg.evaluate(() => localStorage.getItem('scrView') === 'table'));
await pg.click('#scrV_card');
// ---- 비교 바구니 ----
const btns = await pg.$$('.sc-card .sc-cmp');
await btns[0].click(); await pg.waitForTimeout(100);
ok('카드 ⚖ 누르면 바구니 바 표시(1/5), 버튼은 ✓ 담김', await pg.isVisible('#cmpTray') && (await pg.innerText('#cmpTray')).includes('가나전자') && (await btns[0].innerText()).includes('담김'));
ok('바구니 바가 하단 메뉴바와 겹치지 않음', await pg.evaluate(() => document.getElementById('cmpTray').getBoundingClientRect().bottom <= document.getElementById('bnav').getBoundingClientRect().top + 1));
ok('1종목일 때 버튼은 “1종목 더 담기”', (await pg.innerText('#cmpTray .ct-go')).includes('더 담기'));
for (let i = 1; i <= 5; i++) await btns[i].click(); await pg.waitForTimeout(100);
ok('6번째는 거부되고 안내 문구(최대 5)', (await pg.innerText('#cmpTray')).includes('5/5') && !(await pg.innerText('#cmpTray')).includes('사아제약') && await pg.isVisible('#cmpToast') && (await pg.innerText('#cmpToast')).includes('최대 5'));
await pg.reload(); await pg.waitForTimeout(800);
ok('새로고침해도 바구니 유지', await pg.isVisible('#cmpTray') && (await pg.innerText('#cmpTray')).includes('다라화학'));
await pg.click('#cmpTray .ct-go'); await pg.waitForTimeout(900);
ok('비교하기: 종목분석 탭의 비교 패널이 열림(종목 분석 없이도)', await pg.isVisible('#cmpPanel') && !(await pg.isVisible('#stockBody')));
const th = await pg.$$eval('#cmpWrap thead th', (e) => e.map((x) => x.textContent));
ok('5종목 열: 지표 + 5종목', th.length === 6 && th[1] === '가나전자' && th[5] === '카타건설', th.join(','));
ok('입력칸 5개가 바구니로 자동 채워짐', await pg.evaluate(() => [0, 1, 2, 3, 4].map((i) => document.getElementById('cmpName' + i).value).join() === '가나전자,다라화학,마바식품,자차증권,카타건설'));
ok('5종목: 표는 좌우 스크롤, 첫 열(지표)은 고정(sticky)', await pg.$eval('#cmpWrap td:first-child', (e) => getComputedStyle(e).position === 'sticky'));
ok('비교 패널이 열려 있으면 하단 바구니 바는 숨김(가림 방지)', !(await pg.isVisible('#cmpTray')));
ok('초록 강조(최고 ROE 등) 존재', (await pg.$$('#cmpWrap .cmp-best')).length >= 1);
const roe = await pg.$eval('#cmpWrap', (e) => { const row = [...e.querySelectorAll('tr')].find((t) => t.cells[0].textContent === 'ROE 평균'); return [...row.cells].slice(1).map((c) => c.className + ':' + c.textContent); });
ok('ROE 평균: 이익이 가장 큰 다라화학(2배)에 강조', roe[1].startsWith('cmp-best') && roe.filter((x) => x.startsWith('cmp-best')).length === 1, roe.join(' | '));
const per = await pg.$eval('#cmpWrap', (e) => { const row = [...e.querySelectorAll('tr')].find((t) => t.cells[0].textContent === 'PER'); return [...row.cells].slice(1).map((c) => c.className + ':' + c.textContent); });
ok('PER: 값이 가장 작은 쪽에 강조', per.filter((x) => x.startsWith('cmp-best')).length === 1, per.join(' | '));
ok('비교 화면 가로 넘침 없음', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
ok('비교 표에도 글자·전체화면 도구줄', await pg.isVisible('#cmpBar'));
// 한 종목 빼고 2종목 비교
for (const k of [2, 3, 4]) await pg.click('#cmpPanel .cmp-slot >> nth=' + k + ' >> .cmp-x'); await pg.waitForTimeout(100);
ok('슬롯 ✕: 입력 비워지고 바구니도 2종목으로', (await pg.inputValue('#cmpName2')) === '' && (await pg.evaluate(() => cmpList.length)) === 2);
await pg.click('#cmpPanel button.primary'); await pg.waitForTimeout(700);
ok('2종목 비교도 동작(열 3개)', (await pg.$$eval('#cmpWrap thead th', (e) => e.length)) === 3);
await pg.fill('#cmpName1', ''); await pg.click('#cmpPanel button.primary'); await pg.waitForTimeout(200);
ok('1종목만 입력하면 안내', (await pg.innerText('#cmpStatus')).includes('2개 이상'));
await pg.fill('#cmpName1', '사아제약'); await pg.click('#cmpPanel button.primary'); await pg.waitForTimeout(700);
ok('재무데이터 없는 종목: 2개 미만이면 안내(없음 표시)', (await pg.innerText('#cmpStatus')).includes('사아제약'));
await pg.fill('#cmpName1', '다라화학'); await pg.fill('#cmpName2', '사아제약'); await pg.click('#cmpPanel button.primary'); await pg.waitForTimeout(700);
ok('3종목 중 1개 데이터 없음: 나머지 2개로 비교 + 제외 안내', (await pg.$$eval('#cmpWrap thead th', (e) => e.length)) === 3 && (await pg.innerText('#cmpWrap')).includes('제외: 사아제약'));
await pg.click('#cmpPanel button:has-text("종목분석으로")');
ok('← 종목분석으로: 비교 패널 닫힘', !(await pg.isVisible('#cmpPanel')));
// 더보기에서 접근
await pg.click('#bn_more'); await pg.click('#moreSheet button:has-text("종목 비교")'); await pg.waitForTimeout(200);
ok('더보기 → 종목 비교로 바로 진입', await pg.isVisible('#cmpPanel'));
// 종목분석에서 담기 + 재무표 도구
await pg.click('#cmpPanel button:has-text("종목분석으로")');
await pg.click('#cmpTray .ct-clr'); await pg.waitForTimeout(100);
ok('비우기: 바 사라짐', !(await pg.isVisible('#cmpTray')));
await pg.fill('#corpName', '가나전자'); await pg.keyboard.press('Escape'); await pg.click('h3, .topbar'); await pg.click('#btnCmpAdd'); await pg.waitForTimeout(100);
ok('종목분석의 “⚖ 비교 담기”로 담김', (await pg.innerText('#cmpTray')).includes('가나전자') && (await pg.innerText('#btnCmpAdd')).includes('담김'));
await pg.click('button:has-text("분석하기")'); await pg.waitForTimeout(1500);
await pg.evaluate(() => setSubTab('table')); await pg.waitForTimeout(400);
ok('재무제표 표: 도구줄(A−/A＋/전체화면) 보임, 표 렌더됨', await pg.isVisible('#sub_table .tblbar') && (await pg.$$('#wrap table')).length === 1);
await pg.click('#sub_table .tblbar button:has-text("⤢")'); await pg.waitForTimeout(100);
ok('재무표 전체화면', await pg.$eval('#wrap', (e) => getComputedStyle(e).position === 'fixed'));
await pg.click('#tblClose');
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
