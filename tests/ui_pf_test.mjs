// 사용법: node tests/ui_pf_test.mjs  (Playwright + Chromium 필요) — ⑥ 내 계좌 탭: 키 입력 → 현황 → 경고 승인 흐름 → 계산기
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uipf_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
import http from 'node:http'; import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const KEY = 'abcdefghijklmnop'; const posted = [];
const policy = { risk_budget_pct: 0.5, max_position_pct: 15, min_cash_pct: 10, cash_needs_krw: 0, fee_rate_pct: 0.015, adv_limit_pct: 1 };
const state = { policy, trades: [{ id: 1, trade_date: '20250102', side: 'buy', corp_name: '테스트전자', stock_code: '111111', qty: 100, price: 10000, fee: 150, tax: 0, voided: false }], plans: [{ plan_key: 1, version: 1, corp_name: '테스트전자', status: '보유', thesis: '논리', disproof: '반증', entry_low: 9000, entry_high: 10500, risk_price: 8000, tranche_json: '[{"cond":"1차","weightPct":50},{"cond":"2차","weightPct":50}]' }], planVersions: [], journal: [], ledger: { cash: 9000000, realized: 0, fees: 150, deposits: 10000000, errors: [], positions: [{ code: '111111', name: '테스트전자', qty: 100, cost: 1000150, avg: 10001.5, realized: 0 }] },
  summary: { total: 10100000, cash: 9000000, cashPct: 0.89, mv: 1100000, cost: 1000150, unreal: 99850, realized: 0, fees: 150, rows: [{ code: '111111', name: '테스트전자', qty: 100, avg: 10001.5, cost: 1000150, close: 11000, priceDate: '20250102', mv: 1100000, unreal: 99850, unrealPct: 0.0998, sector: '전자', weight: 0.109 }], top1: 0.109, top3: 0.109, top5: 0.109, sectors: [{ name: '전자', mv: 1100000, weight: 0.109 }], warnings: [{ level: 'review', code: 'cash_needs', msg: '현금 경고 테스트' }], partial: false },
  behavior: { disposition: { n: 2, nWin: 1, nLose: 1, enough: false }, activity: { trades90: 1, buys: 1, sells: 0, costTotal: 150, costPct: 0.00001 }, compliance: { buys: 1, planLinked: 1, planLinkedPct: 1, warned: 0, warnedPct: 0, avgdown: 0 }, emotions: { 평온: 1 }, matrix: { goodGood: 0, goodBad: 0, badGood: 0, badBad: 0 }, matrixN: 0 }, asof: 'x', heldWithoutPrice: [] };
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (q.url.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [{ n: '테스트전자', c: '111111' }, { n: '가나다', c: '222222' }] }));
  if (q.url.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [] }));
  if (q.url.startsWith('/api/pf/')) {
    if (q.url.startsWith('/api/pf/quote')) return r.end(JSON.stringify({ corp_name: '테스트전자', stock_code: '111111', close: 11000, date: '20250102', adv20: 5e9, advN: 20 }));
    if (q.headers['x-pf-key'] !== KEY) { r.statusCode = 401; return r.end(JSON.stringify({ error: '접근 키가 맞지 않습니다.' })); }
    if (q.url.startsWith('/api/pf/ping')) return r.end('{"ok":true}');
    if (q.url.startsWith('/api/pf/state')) return r.end(JSON.stringify(state));
    if (q.url.startsWith('/api/pf/trade') && q.method === 'POST') { let b = ''; q.on('data', (c) => b += c); q.on('end', () => { const j = JSON.parse(b); posted.push(j); if (!j.ack) { r.statusCode = 409; return r.end(JSON.stringify({ needs_ack: true, needs_check: true, warnings: [{ id: 'avgdown', level: 'warn', msg: '평균 매입가보다 낮은 추가 매수 테스트' }] })); } r.end(JSON.stringify({ ok: true })); }); return; }
  }
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const port = srv.address().port;
const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] }).catch(async () => chromium.launch({ args: ['--no-sandbox'] }));
const pg = await b.newPage({ viewport: { width: 390, height: 800 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e))); pg.on('console', (m) => { if (m.type() === 'error' && !/404|401|409/.test(m.text())) errs.push(m.text()); });
await pg.goto('http://localhost:' + port + '/'); await pg.waitForTimeout(400);
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
await pg.click('#tabBtnPf'); await pg.waitForTimeout(100);
ok('탭 전환: 키 입력 칸만 보이고 본문은 숨김', await pg.isVisible('#pfuKey') && !(await pg.isVisible('#pfuMain')));
await pg.fill('#pfuKey', 'wrongwrongwrong'); await pg.click('#pfuGate button'); await pg.waitForTimeout(200);
ok('틀린 키 → 오류 문구, 본문 안 열림', (await pg.innerText('#pfuGateMsg')).includes('맞지 않') && !(await pg.isVisible('#pfuMain')));
await pg.fill('#pfuKey', KEY); await pg.click('#pfuGate button'); await pg.waitForTimeout(400);
ok('올바른 키 → 현황 표시', await pg.isVisible('#pfuMain') && (await pg.innerText('#pfuSummary')).includes('10,100,000') && (await pg.innerText('#pfuSummary')).includes('+10.0%'));
ok('현황: 보유 종목이 카드로, 막대와 한도선 표시', (await pg.$$('.pfc')).length === 1 && (await pg.$$('.pfc-bar u')).length === 1);
ok('현황: 위험 기준가 미이탈이면 해당 경고 없음, 서버 경고는 "확인할 것"에 표시', (await pg.innerText('#pfuAlerts')).includes('현금 경고 테스트') && !(await pg.innerText('#pfuAlerts')).includes('위험관리 기준가'));
ok('플로팅 [＋ 기록] 버튼이 보임', await pg.isVisible('#pfuFab'));
await pg.click('.pfc button[data-act="buy"]'); await pg.waitForTimeout(500);
ok('카드의 [매수] → 시트가 열리고 종목명·현재가가 자동 입력', await pg.isVisible('#pfuSheet') && (await pg.inputValue('#pftName')) === '테스트전자' && (await pg.inputValue('#pftPrice')) === '11000');
ok('시트에 종가·보유·계획 상태 문구', (await pg.innerText('#pftCtx')).includes('보유 100주') && (await pg.innerText('#pftCtx')).includes('계획 보유'));
await pg.fill('#pftQty', '10'); await pg.fill('#pftPrice', '9000'); await pg.waitForTimeout(100);
ok('체결 후 비중·현금 미리보기', (await pg.innerText('#pftPrev')).includes('체결 후 비중') && (await pg.innerText('#pftPrev')).includes('현금'), await pg.innerText('#pftPrev'));
await pg.click('#pftEmo button:has-text("불안")');
ok('감정 칩 선택 표시', (await pg.$$eval('#pftEmo button.on', (e) => e.map((x) => x.textContent))).join() === '불안');
await pg.click('#pftGo'); await pg.waitForTimeout(300);
ok('409 → 경고 박스 + 점검표 6개 + 이유 칸', (await pg.innerText('#pftWarn')).includes('추가 매수 테스트') && (await pg.$$('.pftChk')).length === 6);
await pg.fill('#pftAck', '충분히 적은 이유'); await pg.click('#pftWarn button:has-text("알고 기록")'); await pg.waitForTimeout(200);
ok('점검표를 다 체크하지 않으면 전송하지 않음', posted.length === 1);
for (const c of await pg.$$('.pftChk')) await c.check();
await pg.click('#pftWarn button:has-text("알고 기록")'); await pg.waitForTimeout(600);
ok('모두 체크·이유 입력 후 ack 전송(감정 포함)', posted.length === 2 && posted[1].ack === true && posted[1].checks.length === 6 && posted[1].emotion === '불안' && posted[1].ack_reason === '충분히 적은 이유');
ok('기록 후 시트가 닫히고 현황 화면에서 토스트로 결과 확인', !(await pg.isVisible('#pfuSheet')) && (await pg.innerText('#pfuToast')).includes('기록 완료'), await pg.innerText('#pfuToast'));
await pg.click('#pfuFab'); await pg.waitForTimeout(100);
await pg.click('#pftSeg button[data-side="deposit"]');
ok('입금 모드: 종목·수량 칸 숨김, 금액 안내', !(await pg.isVisible('#pftName')) && !(await pg.isVisible('#pftQty')) && (await pg.getAttribute('#pftPrice', 'placeholder')) === '금액(원)');
await pg.click('#pftSeg button[data-side="sell"]'); await pg.fill('#pftName', '테스트전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(500);
ok('매도 모드: 종목명 입력(Enter) → 25/50/전량 빠른 버튼', (await pg.innerText('#pftQuick')).includes('전량'), await pg.innerText('#pftQuick'));
await pg.click('#pftQuick button:has-text("전량")');
ok('전량 버튼 → 보유 수량 자동 입력', (await pg.inputValue('#pftQty')) === '100');
await pg.click('#pfuSheet .pf-sheet-body button:has-text("✕")');
await pg.click('#pfuNb_plan'); await pg.fill('#pfcName', '테스트전자'); await pg.waitForTimeout(300); await pg.fill('#pfcBuy', '10000'); await pg.fill('#pfcRisk', '9000'); await pg.fill('#pfcAdv', '5000000000'); await pg.click('#pfuP_plan button:has-text("계산")'); await pg.waitForTimeout(100);
const co = await pg.innerText('#pfcOut');
// 총자산 10.1M, 위험예산 0.5% = 50,500 / 주당손실 1000 = 50주. 집중도: 15% = 1,515,000 − 1,100,000 = 415,000/10000 = 41주 → 최소 41
ok('계산기: 한도 중 최소(집중도 41주) 채택', co.includes('41주') && co.includes('집중도'), co.slice(0, 120));
ok('계획된 분할 수량(50:50 → 21+20)', co.includes('1차 21주') && co.includes('2차 20주'), co);
await pg.click('#pfuNb_check'); ok('행동 점검: 표본 부족 시 결론 보류 문구', (await pg.innerText('#pfuBeh')).includes('표본이 부족'));
ok('콘솔/페이지 오류 없음', errs.length === 0, JSON.stringify(errs));
await b.close(); srv.close(); console.log(fail ? '\n실패 ' + fail : '\n전부 통과'); process.exit(fail ? 1 : 0);
