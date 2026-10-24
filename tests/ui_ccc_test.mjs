// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 현금전환주기(DSO·DIO·DPO·CCC) 계산과 표시를 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uiccc_' + process.pid + '.mjs');
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
// 모의 재무: 매출 1000g, 매출원가 600g, 매출채권 100g, 재고 90g, 매입채무 60g (g=연도별 배수 → 일수는 연도마다 같음). 2020년은 재고 없음(서비스형), 2019년은 매출원가를 빼 DIO·DPO를 만들 수 없게 함.
rows.forEach((r) => { const g = 1 + (Number(r.bsns_year) - 2018) * 0.1; r.current_assets = 500e8 * g; r.receivables = 100e8 * g; r.inventory = r.bsns_year === '2020' ? null : 90e8 * g; r.payables = 60e8 * g; if (r.bsns_year === '2019') r.cogs = null; });
let pg = await analyze();
const v = await pg.evaluate(() => { const L = AN_CTX.list; const f = (y) => { const d = L.filter((x) => x.year === y)[0]; return d ? { dso: d.dso, dio: d.dio, dpo: d.dpo, ccc: d.ccc } : null; }; return { y24: f(2024), y20: f(2020), y19: f(2019) }; });
const near = (a, b) => typeof a === 'number' && Math.abs(a - b) < 1e-9;
ok('2024: DSO=100/1000×365=36.5, DIO=90/600×365=54.75, DPO=60/600×365=36.5, CCC=54.75', v.y24 && near(v.y24.dso, 36.5) && near(v.y24.dio, 54.75) && near(v.y24.dpo, 36.5) && near(v.y24.ccc, 54.75), JSON.stringify(v.y24));
ok('재고가 없는 해(서비스형, 재고 null)는 DIO=0으로 보고 CCC = 36.5 − 36.5 = 0', v.y20 && near(v.y20.dio, 0) && near(v.y20.ccc, 0), JSON.stringify(v.y20));
ok('매출원가가 없는 해는 DIO·DPO·CCC를 만들지 않음(null), DSO만 계산', v.y19 && v.y19.dio === null && v.y19.dpo === null && v.y19.ccc === null && near(v.y19.dso, 36.5), JSON.stringify(v.y19));
await pg.evaluate(() => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details.card')).filter((x) => x.querySelector('summary').textContent.indexOf('Cash Quality') >= 0)[0]; d.open = true; });
const tt = await pg.evaluate(() => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details.card')).filter((x) => x.querySelector('summary').textContent.indexOf('Cash Quality') >= 0)[0]; return d.innerText; });
ok('Cash Quality 표에 DSO·DIO·DPO·CCC 행과 일 단위 표시', ['매출채권회전일 DSO', '재고회전일 DIO', '매입채무지급일 DPO', '현금전환주기 CCC'].every((k) => tt.includes(k)) && /55일/.test(tt) && /37일|36일/.test(tt) && tt.includes('N/A'), tt.replace(/\s+/g, ' ').slice(0, 120));
const ex = await pg.evaluate(() => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details')).filter((x) => x.querySelector('summary').textContent.indexOf('지표·계산식 설명서') >= 0)[0]; d.open = true; const it = Array.prototype.slice.call(d.querySelectorAll('details')).filter((x) => x.querySelector('summary').textContent.indexOf('현금전환주기') >= 0)[0]; if (it) it.open = true; return it ? it.innerText : ''; });
ok('설명서에 CCC 항목: 계산식·근거(Richards & Laughlin 1980, Deloof 2003)·한계, 해석 단정 금지', ex.includes('DSO = 매출채권') && ex.includes('Richards') && ex.includes('Deloof') && ex.includes('단정하지 않습니다') && ex.includes('정의는 표준'), ex.slice(0, 80));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
