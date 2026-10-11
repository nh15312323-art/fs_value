// 사용법: node tests/ui_mobile_test.mjs  (Playwright + Chromium 필요) — 아이폰 크기(390x844) 화면: 하단 메뉴바, 입력 글자 크기, 터치 크기, 잠금 화면, 개인정보 가림막, 탭 기억
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uimob_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
import http from 'node:http'; import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const KEY = 'abcdefghijklmnop'; const posted = [];
const policy = { risk_budget_pct: 0.5, max_position_pct: 15, min_cash_pct: 10, cash_needs_krw: 0, fee_rate_pct: 0.015, adv_limit_pct: 1 };
const state = { policy, trades: [{ id: 1, trade_date: '20250102', side: 'buy', corp_name: '테스트전자', stock_code: '111111', qty: 100, price: 10000, fee: 150, tax: 0, voided: false }], plans: [{ plan_key: 1, version: 1, corp_name: '테스트전자', status: '보유', thesis: '논리', disproof: '반증', entry_low: 9000, entry_high: 10500, risk_price: 8000, tranche_json: '[{"cond":"1차","weightPct":50},{"cond":"2차","weightPct":50}]' }], planVersions: [], journal: [], ledger: { cash: 9000000, realized: 0, fees: 150, deposits: 10000000, errors: [], positions: [{ code: '111111', name: '테스트전자', qty: 100, cost: 1000150, avg: 10001.5, realized: 0 }] },
  summary: { total: 10100000, cash: 9000000, cashPct: 0.89, mv: 1100000, cost: 1000150, unreal: 99850, realized: 0, fees: 150, rows: [{ code: '111111', name: '테스트전자', qty: 100, avg: 10001.5, cost: 1000150, close: 11000, priceDate: '20250102', mv: 1100000, unreal: 99850, unrealPct: 0.0998, sector: '전자', weight: 0.109 }], top1: 0.109, top3: 0.109, top5: 0.109, sectors: [{ name: '전자', mv: 1100000, weight: 0.109 }], warnings: [{ level: 'review', code: 'cash_needs', msg: '현금 경고 테스트' }], partial: false },
  behavior: { disposition: { n: 2, nWin: 1, nLose: 1, enough: false }, activity: { trades90: 1, buys: 1, sells: 0, costTotal: 150, costPct: 0.00001 }, compliance: { buys: 1, planLinked: 1, planLinkedPct: 1, warned: 0, warnedPct: 0, avgdown: 0 }, emotions: { 평온: 1 }, matrix: { goodGood: 0, goodBad: 0, badGood: 0, badBad: 0 }, matrixN: 0 }, kelly: { n: 40, nWin: 20, nLose: 20, enough: true, p: 0.5, q: 0.5, W: 0.2, L: 0.1, payoff: 2, fRaw: 2.5, f: 1, capped: true, edge: true, half: 1, quarter: 0.625, fLow: 0.9, fHigh: 1, fragile: false }, asof: 'x', heldWithoutPrice: [] };
let PROTECTED = false; const seenKeys = [];
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') { const ok = q.headers['x-pf-key'] === KEY; return r.end(JSON.stringify({ protected: PROTECTED, ok: PROTECTED ? ok : true })); }
  if (p === '/api/screen/top') seenKeys.push(q.headers['x-pf-key'] || '');
  if (PROTECTED && q.headers['x-pf-key'] !== KEY) { r.statusCode = 401; return r.end(JSON.stringify({ error: '접근 키가 맞지 않습니다.' })); }
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [{ n: '테스트전자', c: '111111' }, { n: '가나다', c: '222222' }] }));
  if (p.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [] }));
  if (p === '/api/pf/ping') return r.end('{"ok":true}');
  if (p === '/api/pf/state') return r.end(JSON.stringify(state));
  if (p === '/api/pf/quote') return r.end(JSON.stringify({ corp_name: '테스트전자', stock_code: '111111', close: 10000, date: '20250102', adv20: 5e9, advN: 20 }));
  if (p === '/api/screen/top') return r.end(JSON.stringify({ date: '20250102', dates: [], meta: {}, lists: {} }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const port = srv.address().port, base = 'http://localhost:' + port + '/';
const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] }).catch(async () => chromium.launch({ args: ['--no-sandbox'] }));
const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 3, userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1' });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const open = async (url) => { const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e))); await pg.goto(url || base); await pg.waitForTimeout(500); return pg; };

// ---------- A. 잠금 없는 상태: 레이아웃·입력·메뉴 ----------
let pg = await open();
ok('viewport-fit=cover, 홈 화면 앱 메타·매니페스트·아이콘 링크', await pg.evaluate(() => /viewport-fit=cover/.test(document.querySelector('meta[name=viewport]').content) && !!document.querySelector('link[rel=manifest]') && !!document.querySelector('link[rel=apple-touch-icon]') && document.querySelector('meta[name=apple-mobile-web-app-capable]').content === 'yes'));
ok('확대 제한 없음(접근성): user-scalable/maximum-scale 미사용', await pg.evaluate(() => !/user-scalable|maximum-scale/.test(document.querySelector('meta[name=viewport]').content)));
ok('아이폰 폭: 하단 메뉴바 표시·상단 메뉴 숨김', await pg.isVisible('#bnav') && !(await pg.isVisible('#mainNav')));
const bb = await pg.$eval('#bnav', (e) => e.getBoundingClientRect().height);
ok('하단 메뉴바 높이 ≥ 56px, 버튼 4개 모두 ≥ 44px', bb >= 56 && (await pg.$$eval('#bnav button', (bs) => bs.every((x) => x.getBoundingClientRect().height >= 44 && x.getBoundingClientRect().width >= 44))));
const tabs = ['momentum', 'financial', 'macro', 'data', 'bt', 'pf'];
const tabVia = async (pg, t) => { if (['macro', 'data', 'bt'].includes(t)) { await pg.click('#bn_more'); await pg.click('#mn_' + t); } else await pg.click('#bn_' + t); await pg.waitForTimeout(250); };
if (process.env.DBG) { for (const t of tabs) { await pg.evaluate((t) => switchTab(t), t); await pg.waitForTimeout(300); console.log(t, await pg.evaluate(() => { const o = []; document.querySelectorAll('body *').forEach((e) => { const r = e.getBoundingClientRect(); if (r.width > 0 && r.right > 391 && !e.closest('.tscroll,.combo-list,table,[style*="overflow"]') && getComputedStyle(e).position !== 'fixed') o.push(e.tagName + '#' + e.id + '.' + String(e.className).slice(0, 20) + ' w=' + Math.round(r.width) + ' r=' + Math.round(r.right)); }); return document.documentElement.scrollWidth + ' :: ' + o.slice(0, 8).join(' | '); })); } process.exit(0); }
for (const t of tabs) {
  await tabVia(pg, t);
  const m = await pg.evaluate((t) => {
    const vis = (e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0 && getComputedStyle(e).visibility !== 'hidden'; };
    const root = document.getElementById({ momentum: 'tabMomentum', financial: 'tabFinancial', macro: 'tabMacro', data: 'tabData', bt: 'tabBt', pf: 'tabPf' }[t]);
    const small = [], tiny = [], btn = [];
    root.querySelectorAll('input:not([type=checkbox]):not([type=radio]):not([type=hidden]):not([type=range]), select, textarea').forEach((e) => { if (!vis(e)) return; if (parseFloat(getComputedStyle(e).fontSize) < 16) small.push(e.id || e.className || e.tagName); if (e.getBoundingClientRect().height < 43.5) tiny.push((e.id || e.tagName) + ':' + Math.round(e.getBoundingClientRect().height)); });
    root.querySelectorAll('button').forEach((e) => { if (!vis(e) || e.closest('table')) return; if (e.getBoundingClientRect().height < 39.5) btn.push((e.id || e.textContent.trim().slice(0, 10)) + ':' + Math.round(e.getBoundingClientRect().height)); });
    return { small, tiny, btn, over: document.documentElement.scrollWidth - innerWidth, active: document.querySelector('#bnav button.on') && document.querySelector('#bnav button.on').id, hash: location.hash };
  }, t);
  ok(t + ': 입력칸 글자 16px 이상(아이폰 자동 확대 방지)', m.small.length === 0, m.small.join(','));
  ok(t + ': 입력칸 높이 44px 이상', m.tiny.length === 0, m.tiny.join(','));
  ok(t + ': 버튼 높이 40px 이상', m.btn.length === 0, m.btn.join(' | '));
  ok(t + ': 가로 넘침 없음', m.over <= 1, String(m.over));
  ok(t + ': 하단바 강조·주소 #탭 기록', m.hash === '#' + t && m.active === ({ macro: 'bn_more', data: 'bn_more', bt: 'bn_more' }[t] || 'bn_' + t), m.active + ' ' + m.hash);
}
// 뒤로가기 / 링크 / 마지막 탭 기억
await tabVia(pg, 'financial'); await tabVia(pg, 'momentum');
await pg.goBack(); await pg.waitForTimeout(300);
ok('뒤로가기: 이전 탭(종목분석)으로 이동', await pg.isVisible('#tabFinancial') && !(await pg.isVisible('#tabMomentum')));
const pg2 = await open(base + '#data');
ok('#data 링크로 열면 데이터 관리 탭', await pg2.isVisible('#tabData'));
const pg3 = await open(base);
ok('마지막으로 본 탭 기억(해시 없이 열면 data)', await pg3.isVisible('#tabData'));
await pg2.close(); await pg3.close();
// 더보기 시트 닫기
await pg.click('#bn_more'); ok('더보기 시트 열림', await pg.isVisible('#moreSheet .ms-body')); await pg.mouse.click(195, 100); await pg.waitForTimeout(150);
ok('바깥을 누르면 닫힘', !(await pg.isVisible('#moreSheet .ms-body')));
ok('escHtml: 따옴표·꺾쇠 이스케이프', await pg.evaluate(() => escHtml('<a href="x" onclick=\'y\'>&') === '&lt;a href=&quot;x&quot; onclick=&#39;y&#39;&gt;&amp;'));
// 입력 효율 속성
await tabVia(pg, 'financial');
ok('종목 검색창: 검색 엔터키·자동대문자 끔', await pg.evaluate(() => { const e = document.getElementById('corpName'); return e.getAttribute('enterkeyhint') === 'search' && e.getAttribute('autocapitalize') === 'off'; }));
ok('가로 스크롤 없음(종목분석 초기)', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
await pg.close();

// ---------- B. 접근 키 보호 상태 ----------
PROTECTED = true;
pg = await open();
ok('보호 상태: 키 없으면 잠금 화면 표시', await pg.isVisible('#lockScreen') && await pg.isVisible('#lkKey'));
ok('잠금 화면 입력칸 16px 이상', await pg.$eval('#lkKey', (e) => parseFloat(getComputedStyle(e).fontSize) >= 16));
await pg.fill('#lkKey', 'wrongwrongwrong'); await pg.click('#lockScreen button.primary'); await pg.waitForTimeout(300);
ok('틀린 키: 오류 문구, 계속 잠김', (await pg.innerText('#lkMsg')).includes('맞지 않') && await pg.isVisible('#lockScreen'));
await pg.fill('#lkKey', KEY); await pg.check('#lkRem'); await pg.press('#lkKey', 'Enter'); await pg.waitForTimeout(500);
ok('올바른 키(엔터): 잠금 화면 닫힘', !(await pg.isVisible('#lockScreen')));
ok('기억하기 체크 시 localStorage에 만료일과 함께 저장', await pg.evaluate(() => { const r = JSON.parse(localStorage.getItem('pfKeyR')); return r && r.k && r.exp > Date.now() + 29 * 86400000; }));
ok('화면 소스(HTML)에 키가 들어 있지 않음', !HTML_PAGE.includes(KEY));
seenKeys.length = 0; await tabVia(pg, 'momentum'); await pg.waitForTimeout(300);
ok('모든 /api 호출에 키가 자동 첨부', seenKeys.length > 0 && seenKeys.every((k) => k === KEY));
await tabVia(pg, 'pf'); await pg.waitForTimeout(600);
ok('내 계좌: 별도 입력 없이 자동으로 열림', await pg.isVisible('#pfuMain') && (await pg.innerText('#pfuSummary')).includes('10,100,000'));
// 금액 → 수량
await pg.evaluate(() => pfuOpenSheet('buy')); await pg.waitForTimeout(150);
await pg.fill('#pftPrice', '10000'); await pg.fill('#pftAmt', '1050000'); await pg.waitForTimeout(100);
ok('금액→수량 자동 계산(1,050,000 / 10,000 = 105주)', (await pg.inputValue('#pftQty')) === '105' && (await pg.innerText('#pftAmtHint')).includes('105'));
const gb = await pg.$eval('#pftGo', (e) => e.getBoundingClientRect().bottom);
ok('거래 시트: 기록 버튼이 화면 안에 고정', gb <= 844 && gb > 600, String(gb));
ok('거래 시트 입력칸 16px 이상', await pg.$$eval('#pfuSheet input:not([type=checkbox])', (es) => es.filter((e) => e.getBoundingClientRect().width > 0).every((e) => parseFloat(getComputedStyle(e).fontSize) >= 16)));
await pg.evaluate(() => pfuCloseSheet());
// 개인정보 가림막
await pg.evaluate(() => { Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
ok('앱 전환(백그라운드) 시 계좌 화면 가림막 표시', await pg.isVisible('#privCover'));
await pg.evaluate(() => { Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
ok('돌아오면 가림막 해제(5분 미만)', !(await pg.isVisible('#privCover')) && !(await pg.isVisible('#lockScreen')));
// 새로고침해도 기억된 키로 자동 진입
await pg.reload(); await pg.waitForTimeout(700);
ok('새로고침: 기억된 키로 잠금 없이 바로 진입, 마지막 탭(내 계좌) 복원', !(await pg.isVisible('#lockScreen')) && await pg.isVisible('#tabPf'));
// 잠금
await pg.click('#pfuNb_lock'); await pg.waitForTimeout(200);
ok('잠금 버튼: 잠금 화면 + 저장된 키·계좌 내용 삭제', await pg.isVisible('#lockScreen') && await pg.evaluate(() => !localStorage.getItem('pfKeyR') && !sessionStorage.getItem('pfKey') && document.getElementById('pfuSummary').innerHTML === ''));
// 5분 이상 자리 비움 (기억 안 함 상태)
await pg.fill('#lkKey', KEY); await pg.click('#lockScreen button.primary'); await pg.waitForTimeout(500);
ok('기억 안 함으로 다시 열기', !(await pg.isVisible('#lockScreen')) && await pg.evaluate(() => !localStorage.getItem('pfKeyR') && !!sessionStorage.getItem('pfKey')));
await pg.evaluate(() => { Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); lockedAt = Date.now() - 6 * 60000; Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true }); document.dispatchEvent(new Event('visibilitychange')); });
await pg.waitForTimeout(150);
ok('5분 넘게 자리를 비우면 자동 잠금', await pg.isVisible('#lockScreen'));
// 키가 서버에서 거부되면(만료·교체) 잠금 화면
await pg.fill('#lkKey', KEY); await pg.click('#lockScreen button.primary'); await pg.waitForTimeout(400);
await pg.evaluate(() => { sessionStorage.setItem('pfKey', 'stale-stale-stale'); PFU.key = 'stale-stale-stale'; });
await pg.evaluate(() => { fetch('/api/screen/top'); }); await pg.waitForTimeout(400);
ok('저장된 키가 거부되면(401) 잠금 화면으로', await pg.isVisible('#lockScreen'));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
