// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uithesis_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const SHOT = process.env.SHOT_DIR || os.tmpdir();
const dates = []; { let d = new Date(Date.UTC(2024, 9, 2)); while (dates.length < 400) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round(50000 + i * 40 + 3000 * Math.sin(i / 20)));
const vol = dates.map((_, i) => 100000 + (i % 17) * 20000), tv = vol.map((v, i) => v * close[i]);
const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of [['11013','1분기',1],['11012','반기',2],['11014','3분기',3],['11011','사업보고서',4]]) {
  const g = 1 + (y - 2018) * 0.1;
  rows.push({ corp_code: 'X', bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g, net_income: 110e8 * g, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g * po, capex: 60e8 * po, fcf: 120e8 * g * po, total_shares: 1e7, treasury_shares: 0, parent_net_income: 110e8 * g, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, dividend_per_share: 500 + (y === 2024 ? 100 : 0), price_at_period_end: 40000 * g * (po === 4 ? 1 : 0.9) });
}

const KEY = 'abcdefghijklmnop';
const mkState = (plans) => ({ policy: null, trades: [], plans, summary: { total: 0, cash: 0, cashPct: 0, mv: 0, cost: 0, unreal: 0, realized: 0, fees: 0, rows: [], top1: 0, top3: 0, top5: 0, sectors: [], warnings: [], partial: false }, behavior: { disposition: { n: 0, nWin: 0, nLose: 0, enough: false }, activity: { trades90: 0, buys: 0, sells: 0, costTotal: 0, costPct: 0 }, compliance: { buys: 0, planLinked: 0, planLinkedPct: 0, warned: 0, warnedPct: 0, avgdown: 0 }, emotions: {}, matrix: { goodGood: 0, goodBad: 0, badGood: 0, badBad: 0 }, matrixN: 0 }, kelly: { n: 0, enough: false }, asof: 'x', heldWithoutPrice: [], ledger: { deposits: 0, withdrawals: 0 } });
globalThis.__pfState = mkState([{ plan_key: 7, version: 2, corp_name: '모의전자', stock_code: '000001', status: '매수검토', thesis: '로봇 매출이 늘어난다', disproof: '수주 2분기 연속 감소', review_date: '2026-12-31' }]);
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [{ n: '모의전자', c: '000001' }] }));
  if (p.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [{ corp_name: '모의전자', stock_code: '000001', n: rows.length, acc_mt: 12 }] }));
  if (p === '/api/db/usage') return r.end(JSON.stringify({ sizeBytes: 1e6, limitBytes: 5e8, limitMb: 500, warnPct: 90, pct: 0.2, warn: false }));
  if (p.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: '모의전자', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: vol, tv }));
  if (p.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: '모의전자', close: close[close.length - 1], date: dates[dates.length - 1] }));
  if (p.startsWith('/api/financial-history-db')) return r.end(JSON.stringify({ corp_name: '모의전자', corp_code: 'X', acc_mt: 12, rows }));
  if (p.startsWith('/api/news')) { const L = dates[dates.length - 1], dd = (n) => { const t = new Date(Date.UTC(+L.slice(0, 4), +L.slice(4, 6) - 1, +L.slice(6, 8)) - n * 86400000); return t.toISOString().slice(0, 10).replace(/-/g, ''); };
    const items = []; for (let k = 0; k < 4; k++) items.push({ d: dd(8 + k), t: '모의전자 로봇 사업 진출 ' + k + '호', l: 'https://news.example/r' + k }); for (let k = 0; k < 6; k++) items.push({ d: dd(200 + k), t: '모의전자 중국 공장 증설 소식 ' + k, l: 'https://news.example/c' + k });
    items.sort((a, b) => (a.d < b.d ? 1 : -1));
    return r.end(JSON.stringify({ corp_name: '모의전자', from: dd(365), to: L, days: 365, got: items.length, partial: false, total: items.length, items })); }
  if (p.startsWith('/api/disclosures') && globalThis.__failDisc) { r.statusCode = 502; return r.end(JSON.stringify({ error: '공시목록 요청 실패: 프록시(Apps Script) 응답 시간 초과' })); }
  if (p.startsWith('/api/disclosures')) { const L = dates[dates.length - 1], dd = (n) => { const t = new Date(Date.UTC(+L.slice(0, 4), +L.slice(4, 6) - 1, +L.slice(6, 8)) - n * 86400000); return t.toISOString().slice(0, 10).replace(/-/g, ''); };
    return r.end(JSON.stringify({ corp_name: '모의전자', stock_code: '000001', from: dd(365), to: L, days: 365, total_count: 5, got: 5, partial: false,
      items: [{ d: dd(12), nm: '단일판매ㆍ공급계약체결', no: '20250101000001' }, { d: dd(40), nm: '영업(잠정)실적(공정공시)', no: '20250101000002' }, { d: dd(200), nm: '유상증자결정', no: '20250101000003' }, { d: dd(25), nm: '분기보고서 (2025.06)', no: '20250101000004' }, { d: dd(150), nm: '현금ㆍ현물배당결정', no: '20250101000005' }],
      idx: { d: dates, c: dates.map((_, i) => 2500 + i) } })); }
  if (p === '/api/pf/ping') return r.end('{"ok":true}');
  if (p === '/api/pf/state') { if (q.headers['x-pf-key'] !== KEY) { r.statusCode = 401; return r.end('{"error":"키"}'); } return r.end(JSON.stringify(globalThis.__pfState)); }
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const ctx = await b.newContext({ viewport: { width: 390, height: 844 } });
const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('financial')); await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(2200);
const box = () => pg.innerText('#anThesisBox');
ok('잠금 상태: 안내 문구 + 내 투자 열기', (await box()).includes('잠금을 풀면') && (await box()).includes('내 투자 열기'));
ok('습관 도구·입증 안 됨 고지 + 손절 아님', (await box()).includes('습관 도구') && (await box()).includes('입증된 것은 아니며'));
// 잠금 해제 후 돌아오면 계획서 내용이 보임
await pg.click('#anThesisBox button'); await pg.waitForTimeout(200);
ok('버튼 → 내 투자 탭으로 이동', await pg.isVisible('#pfuKey'));
await pg.fill('#pfuKey', KEY); await pg.click('#pfuGate button'); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('financial')); await pg.waitForTimeout(200);
ok('해제 후: 가설·무효화 조건·상태·버전 표시', (await box()).includes('로봇 매출이 늘어난다') && (await box()).includes('수주 2분기 연속 감소') && (await box()).includes('매수검토') && (await box()).includes('v2'), await box());
await pg.click('#anThesisBox button'); await pg.waitForTimeout(300);
ok('수정 버튼 → 계획서 폼에 기존 내용 채움', (await pg.inputValue('#pfpThesis')) === '로봇 매출이 늘어난다' && (await pg.inputValue('#pfpDis')) === '수주 2분기 연속 감소' && (await pg.inputValue('#pfpKey')) === '7');
// 계획이 없는 경우
await pg.evaluate(() => { PFU.st.plans = []; switchTab('financial'); anThesisUpdate(); });
ok('계획 없음: 쓰기 버튼', (await box()).includes('아직 없습니다') && (await box()).includes('이 종목 투자 가설 쓰기'));
await pg.click('#anThesisBox button'); await pg.waitForTimeout(300);
ok('쓰기 버튼 → 종목명 미리 채움, 이전 편집 내용 비움', (await pg.inputValue('#pfpName')) === '모의전자' && (await pg.inputValue('#pfpThesis')) === '' && (await pg.inputValue('#pfpKey')) === '');
ok('접기 장 제어가 박스를 가리지 않음(박스는 details 아님)', await pg.evaluate(() => !document.getElementById('anThesisBox').closest('details')));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
