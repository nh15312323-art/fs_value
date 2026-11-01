// 사용법: node tests/ui_stable_test.mjs (Playwright + Chromium 필요)
// 종목을 연달아 바꿔 조회해도 화면 구성이 달라지지 않는지(자료가 부족한 종목·오류·계산 예외·응답 지연 포함) 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uistable_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const SHOT = process.env.SHOT_DIR || os.tmpdir();
const dates = []; { let d = new Date(Date.UTC(2024, 9, 2)); while (dates.length < 400) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round(50000 + i * 40 + 3000 * Math.sin(i / 20)));
const vol = dates.map((_, i) => 100000 + (i % 17) * 20000), tv = vol.map((v, i) => v * close[i]);
const mkRows = (code, o) => { o = o || {}; const out = []; for (let y = o.y0 || 2018; y <= (o.y1 || 2024); y++) for (const [rc, lab, po] of [['11013','1분기',1],['11012','반기',2],['11014','3분기',3],['11011','사업보고서',4]]) {
  if (o.annualOnly && po !== 4) continue; if (o.noAnnual && po === 4) continue;
  const g = (1 + (y - 2018) * 0.1) * (o.scale || 1), neg = o.loss ? -1 : 1;
  const r = { corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g * neg, net_income: 110e8 * g * neg, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g * po, capex: 60e8 * po, fcf: 120e8 * g * po, total_shares: 1e7, treasury_shares: 0, parent_net_income: 110e8 * g * neg, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, price_at_period_end: 40000 * g };
  if (o.sparse) { ['cogs', 'ocf', 'capex', 'fcf', 'total_liabilities', 'cash', 'total_shares', 'filing_date', 'price_at_period_end', 'parent_net_income'].forEach((k) => { r[k] = null; }); }
  out.push(r); } return out; };
const STK = {
  '가전자': { code: 'A', stock: '000001', rows: mkRows('A') },
  '나화학': { code: 'B', stock: '000002', rows: mkRows('B', { scale: 2, y0: 2020 }) },
  '다분기': { code: 'C', stock: '000003', rows: mkRows('C', { noAnnual: true }) },
  '라연간': { code: 'D', stock: '000004', rows: mkRows('D', { annualOnly: true }) },
  '마적자': { code: 'E', stock: '000005', rows: mkRows('E', { loss: true }) },
  '바희소': { code: 'F', stock: '000006', rows: mkRows('F', { sparse: true }) },
  '사결산': { code: 'G', stock: '000007', rows: mkRows('G'), acc_mt: 3 },
  '아무가격': { code: 'H', stock: '000008', rows: mkRows('H'), noPrice: true },
};
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname, nm = u.searchParams.get('corp_name');
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: Object.keys(STK).map((n) => ({ n, c: STK[n].stock })) }));
  if (p.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: Object.keys(STK).map((n) => ({ corp_code: STK[n].code, corp_name: n, stock_code: STK[n].stock, n: STK[n].rows.length, acc_mt: STK[n].acc_mt || 12, first_year: 2018, last_year: 2024 })) }));
  if (p === '/api/db/usage') return r.end(JSON.stringify({ sizeBytes: 1e6, limitBytes: 5e8, limitMb: 500, warnPct: 90, pct: 0.2, warn: false }));
  const s = STK[nm];
  if (p.startsWith('/api/price/series')) { if (!s || s.noPrice) { r.statusCode = 404; return r.end(JSON.stringify({ error: '시세 없음' })); } return r.end(JSON.stringify({ corp_name: nm, stock_code: s.stock, n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: vol, tv })); }
  if (p.startsWith('/api/latest-price')) { if (!s || s.noPrice) { r.statusCode = 404; return r.end(JSON.stringify({ error: '시세 없음' })); } const send = () => r.end(JSON.stringify({ corp_name: nm, close: nm === '가전자' ? 12345 : close[close.length - 1], date: dates[dates.length - 1] })); if (nm === '가전자') return setTimeout(send, 900); return send(); }
  if (p.startsWith('/api/financial-history-db')) { if (nm === '오류주') { r.statusCode = 500; return r.end(JSON.stringify({ error: '서버 오류(모의)' })); } if (!s) { r.statusCode = 404; return r.end(JSON.stringify({ error: '없음' })); } return r.end(JSON.stringify({ corp_name: nm, corp_code: s.code, stock_code: s.stock, acc_mt: s.acc_mt || 12, rows: s.rows })); }
  if (p.startsWith('/api/news')) return r.end(JSON.stringify({ corp_name: nm, from: dates[0], to: dates[dates.length - 1], days: 365, got: 0, partial: false, total: 0, items: [] }));
  if (p.startsWith('/api/disclosures')) return r.end(JSON.stringify({ corp_name: nm, stock_code: '', from: dates[0], to: dates[dates.length - 1], days: 365, total_count: 0, got: 0, partial: false, items: [], idx: { d: dates, c: dates.map((_, i) => 2500 + i) } }));
  if (p.startsWith('/api/screen/top')) { r.statusCode = 500; return r.end(JSON.stringify({ error: '스크리닝 서버 오류(모의)' })); }
  if (p.startsWith('/api/industry/lookup')) return r.end('{"map":{}}');
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage();
pg.on('pageerror', (e) => errs.push('pageerror: ' + String(e))); const logged = []; pg.on('console', (m) => { if (m.type() !== 'error' || /404|500|Failed to load resource/.test(m.text())) return; if (m.text().indexOf('[화면 그리기 오류]') === 0) logged.push(m.text()); else errs.push('console: ' + m.text()); });
await pg.goto(base); await pg.waitForTimeout(500); await pg.evaluate(() => switchTab('financial'));

const go = async (name, wait) => { await pg.evaluate((n) => { document.getElementById('corpName').value = n; analyzeStock(); }, name); await pg.waitForTimeout(wait || 2400); };
const snap = () => pg.evaluate(() => {
  const vis = (id) => { const e = document.getElementById(id); return !!e && e.offsetParent !== null; };
  const body = document.getElementById('stockBody');
  return { body: vis('stockBody'), kids: Array.prototype.map.call(body.children, (c) => c.id).join('>'), mat: !!document.querySelector('#matSlot details'), matOpen: !!(document.querySelector('#matSlot details') || {}).open,
    summaryOk: !!document.querySelector('#summary #analysis details'), summaryTxt: document.getElementById('summary').innerText.slice(0, 80), five: document.getElementById('fiveStepWrap').innerText.length > 30, tbl: document.getElementById('wrap').innerHTML.length > 500,
    subVis: ['summary', 'table', 'five'].filter((k) => vis('sub_' + k)).join(), key: AN_STATE.key, raw0: rawRows[0] ? rawRows[0].corp_code : null, price: document.getElementById('currentPrice').value, status: document.getElementById('status').innerText };
});
const seq = ['가전자', '나화학', '다분기', '라연간', '마적자', '바희소', '사결산', '아무가격', '가전자'];
let sig0 = null;
for (const n of seq) {
  await go(n); const s = await snap(); const code = STK[n].code, noAnnual = n === '다분기';
  if (sig0 === null) sig0 = s.kids;
  ok(n + ': 화면 뼈대 동일(구성 요소 순서) + 재료 점검 칸 있음', s.body && s.kids === sig0 && s.mat, s.kids);
  ok(n + ': 요약·분석이 ' + (noAnnual ? '없고 안내 문구' : '현재 종목 것') , noAnnual ? (!s.summaryOk && s.summaryTxt.includes('요약을 만들 수 없습니다') && s.key === null) : (s.summaryOk && s.key === code && s.raw0 === code), JSON.stringify({ k: s.key, r: s.raw0, t: s.summaryTxt.slice(0, 20) }));
  ok(n + ': 표·5단계 표시, 요약 탭만 보임', s.tbl && s.five !== undefined && s.subVis === 'summary', s.subVis);
}
// 오류 종목 → 이전 종목 화면이 남지 않음
await go('오류주', 1200); let e = await snap();
ok('조회 실패 종목: 본문 숨김, 이전 종목의 요약·재료 점검이 남지 않음, 오류 문구', !e.body && !e.summaryOk && !e.mat && e.status.length > 0 && e.raw0 === null, JSON.stringify({ b: e.body, m: e.mat, st: e.status.slice(0, 30), r: e.raw0 }));
await go('나화학'); e = await snap();
ok('오류 뒤 정상 종목: 뼈대·요약 정상 복구', e.body && e.kids === sig0 && e.summaryOk && e.key === 'B');
// 계산 예외가 나도 나머지 화면은 그대로
await pg.evaluate(() => { window.__cfh = computeFactorHistory; computeFactorHistory = () => { throw new Error('모의 계산 오류'); }; });
const errsBefore = errs.length;
await go('마적자'); e = await snap();
const warn = await pg.evaluate(() => document.getElementById('summary').innerText);
ok('요약 계산 예외: 오류 안내 카드만 뜨고 재료 점검·표·5단계·하위 탭은 정상', e.body && e.kids === sig0 && e.mat && e.tbl && e.five && e.subVis === 'summary' && warn.includes('표시하는 중 오류'), warn.slice(0, 80));
ok('예외가 미처리 오류로 번지지 않고 콘솔에 한 번 기록됨', errs.length === errsBefore && logged.length === 1, errs.slice(errsBefore).join(';') + ' logged=' + logged.length);
await pg.evaluate(() => { computeFactorHistory = window.__cfh; });
await go('라연간'); e = await snap();
ok('예외 해소 후 다음 종목은 정상', e.summaryOk && e.key === 'D' && e.mat);
// 재료 점검 열림 상태 유지 + 종목별로 새로 불러옴
await pg.evaluate(() => { document.querySelector('#matSlot details').open = true; }); await pg.waitForTimeout(900);
ok('재료 점검을 열면 현재 종목 이름으로 불러옴', await pg.evaluate(() => MAT_ST.name === '라연간' && !!MAT_ST.data));
await go('가전자', 2600); e = await snap();
ok('다른 종목으로 바꿔도 재료 점검은 열린 채 유지, 새 종목 것으로 갱신', e.matOpen && await pg.evaluate(() => MAT_ST.name === '가전자'), JSON.stringify({ o: e.matOpen }));
await pg.evaluate(() => { document.querySelector('#matSlot details').open = false; });
await go('나화학'); e = await snap();
ok('닫아 두면 다음 종목에서도 닫힌 채 칸은 유지', e.mat && !e.matOpen);
// 응답 지연: 느린 종목의 현재가가 다음 종목에 들어오지 않음
await pg.evaluate(() => { document.getElementById('corpName').value = '가전자'; analyzeStock(); });
await pg.waitForTimeout(150);
await pg.evaluate(() => { document.getElementById('corpName').value = '나화학'; analyzeStock(); });
await pg.waitForTimeout(2800); e = await snap();
ok('응답이 늦은 이전 종목(가전자 12345원)의 가격이 나화학 입력칸에 들어가지 않음', e.price !== '12345' && e.key === 'B' && e.raw0 === 'B', e.price + ' ' + e.key);
// 하위 탭 선택 유지
await pg.evaluate(() => setSubTab('table')); await go('다분기'); e = await snap();
ok('하위 탭(재무제표 표) 선택은 종목을 바꿔도 유지', e.subVis === 'table', e.subVis);
await pg.evaluate(() => setSubTab('summary'));

// ---- 다른 화면: 실패·예외가 나도 이전 결과가 남거나 화면이 사라지지 않음
await pg.evaluate(() => switchTab('momentum')); await pg.waitForTimeout(300);
await pg.evaluate(() => { document.getElementById('screenCards').innerHTML = '<div id="oldCard">이전 카드</div>'; document.getElementById('screenWrap').innerHTML = '<table><tr><td>이전 표</td></tr></table>'; return loadScreen('top'); });
await pg.waitForTimeout(500);
const sc = await pg.evaluate(() => ({ cards: document.getElementById('screenCards').innerText, wrap: document.getElementById('screenWrap').innerText, reg: document.getElementById('screenRegime').innerText, st: document.getElementById('screenStatus').innerText }));
ok('스크리닝 조회 실패 시 이전 결과 카드·표가 남지 않고 오류·안내 표시', !sc.cards.includes('이전') && !sc.wrap.includes('이전') && sc.reg.includes('결과가 없습니다') && sc.st.includes('오류'), JSON.stringify(sc).slice(0, 160));
await pg.evaluate(() => switchTab('fscr')); await pg.waitForTimeout(200);
await pg.evaluate(() => { FSC.rows = [null]; fscRender(); });
ok('재무 스크리닝 그리기 예외 → 오류 안내 카드(화면이 비지 않음)', (await pg.innerText('#fscOut')).includes('표시하는 중 오류'));
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(500);
await pg.evaluate(() => { BT.stocks = [{ code: 'Z', name: 'Z', rows: null }]; return btRunFin(); }); await pg.waitForTimeout(400);
ok('백테스트 계산 예외 → 오류 안내 카드 + 상태 문구', (await pg.innerText('#btFinOut')).includes('표시하는 중 오류') && (await pg.innerText('#btStatus')).includes('오류'));
ok('처리되지 않은 페이지 오류 없음', errs.length === 0, errs.join(';'));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
