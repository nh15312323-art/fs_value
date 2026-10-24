// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uifold_' + process.pid + '.mjs');
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
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 2 });
const analyze = async () => { const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e))); await pg.goto(base); await pg.waitForTimeout(500); await pg.evaluate(() => switchTab('financial')); await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(2200); return pg; };
const openSec = async (pg, key) => { await pg.evaluate((k) => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details.card')).filter((x) => x.querySelector('summary').textContent.indexOf(k) >= 0)[0]; if (d) { d.open = true; d.scrollIntoView(); } }, key); await pg.waitForTimeout(150); };
let pg = await analyze();
if (process.env.SHOT) { await pg.evaluate(() => document.getElementById('analysis').scrollIntoView()); await pg.screenshot({ path: process.env.SHOT + '_fold.png' }); }
const st = async () => pg.evaluate(() => { const ds = Array.prototype.slice.call(document.querySelectorAll('#analysis > details')); return { total: ds.length, open: ds.filter((d) => d.open).map((d) => d.querySelector('summary').textContent.trim().slice(0, 12)) }; });
ok('한눈에 보기 카드가 맨 위, 4개 축(Growth·Quality·Valuation·Momentum)을 따로 표시', await pg.evaluate(() => { const g = document.getElementById('anGlance'); const fd = document.querySelector('#analysis > details'); return !!g && !!(g.compareDocumentPosition(fd) & Node.DOCUMENT_POSITION_FOLLOWING) && ['Growth', 'Quality', 'Valuation', 'Momentum'].every((k) => g.innerText.includes(k)) && g.innerText.includes('합산하지 않고'); }));
const gt = await pg.innerText('#anGlance');
ok('데이터 기준 줄: 최근 연간 2024년 · 연결재무제표 · 공시일 · 연간 개년 수 · 현재가', gt.includes('최근 연간 2024년') && gt.includes('연결재무제표') && gt.includes('공시일') && /연간 \d+개년/.test(gt), gt.replace(/\n/g, ' ').slice(0, 200));
const s0 = await st();
ok('처음에는 종합 분류·투자 판단 요약만 펼침(나머지는 접힘)', s0.total > 10 && s0.open.length === 2 && s0.open.some((t) => t.includes('종합 분류')) && s0.open.some((t) => t.includes('투자 판단')), JSON.stringify(s0));
ok('장 머리글은 항목 개수와 함께 펼치기 안내, 근거 문구는 접혀 있음', await pg.evaluate(() => { const c = document.querySelector('#analysis .an-chap'); return /항목 \d+개 펼치기/.test(c.innerText) && c.querySelector('details') && !c.querySelector('details').open; }));
await pg.evaluate(() => { document.querySelectorAll('#analysis .an-chap')[2].firstElementChild.click(); });
const s1 = await st(); const n1 = s1.open.length;
ok('장 머리글을 누르면 그 장의 항목이 모두 펼쳐지고 안내가 접기로 바뀜', n1 > 2 && await pg.evaluate(() => document.querySelectorAll('#analysis .an-chap')[2].innerText.includes('항목 접기')), JSON.stringify(s1));
await pg.evaluate(() => { document.querySelectorAll('#analysis .an-chap')[2].firstElementChild.click(); });
ok('다시 누르면 그 장만 접힘', (await st()).open.length === 2);
await pg.evaluate(() => anGoChap('⑥')); await pg.waitForTimeout(500);
const s2 = await st();
ok('한눈에 보기의 질문 버튼(⑥)을 누르면 해당 장이 펼쳐짐', s2.open.length > 2 && s2.open.some((t) => /시나리오|가치평가|Valuation/.test(t)), JSON.stringify(s2));
// 가격을 바꿔 다시 그려도 사용자가 연 항목 유지
const before = (await st()).open.slice().sort().join('|');
await pg.evaluate(() => renderAnalysis()); await pg.waitForTimeout(300);
ok('같은 종목을 다시 그려도 사용자가 연 항목이 유지됨', (await st()).open.slice().sort().join('|') === before);
await pg.click('button:has-text("전체 접기")'); await pg.waitForTimeout(100);
ok('상단 막대의 전체 접기/펼치기도 그대로 동작', (await st()).open.length === 0);
await pg.click('button:has-text("전체 펼치기")'); await pg.waitForTimeout(100);
ok('전체 펼치기', (await st()).open.length === s0.total);
const over = await pg.evaluate(() => document.documentElement.scrollWidth - innerWidth);
ok('모바일 가로 넘침 없음', over <= 1, String(over));
if (process.env.SHOT) { await pg.evaluate(() => { document.querySelectorAll('#analysis details').forEach((d) => { d.open = false; }); window.scrollTo(0, 0); }); await pg.evaluate(() => renderAnalysis()); }
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
