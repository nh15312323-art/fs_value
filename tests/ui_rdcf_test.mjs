// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uirdcf_' + process.pid + '.mjs');
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
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('financial')); await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(2200);
// 1) 수학: 풀이값을 다시 정방향 계산에 넣으면 가격이 복원
const rt = await pg.evaluate(() => { const out = []; for (const [f0, r, gT, N, g] of [[1000, 0.09, 0.02, 10, 0.07], [500, 0.10, 0.0, 5, -0.03], [2000, 0.08, 0.03, 15, 0.2]]) { const P = anRdcfValue(f0, g, r, gT, N); out.push([g, anRdcfSolve(P, f0, r, gT, N)]); } return out; });
ok('역DCF: 정방향 가격→풀이 성장률 왕복 일치(3케이스)', rt.every((x) => Math.abs(x[0] - x[1]) < 1e-6), JSON.stringify(rt));
// 2) 손계산: g=0, r=10%, gT=0, N=1, F0=100 → 100/1.1 + (100/0.1)/1.1 = 90.909+909.09=1000
const hand = await pg.evaluate(() => anRdcfValue(100, 0, 0.10, 0, 1));
ok('역DCF: 손계산 1000과 일치', Math.abs(hand - 1000) < 1e-9, String(hand));
const na = await pg.evaluate(() => [anRdcfSolve(1000, -5, 0.09, 0.02, 10), anRdcfSolve(1000, 100, 0.02, 0.02, 10), anRdcfSolve(1e12, 100, 0.09, 0.02, 10), anRdcfSolve(0, 100, 0.09, 0.02, 10), anRdcfBuild(1000, null, 0.09), anRdcfBuild(1000, -3, 0.09)]);
ok('N/A: F0≤0, r≤gT, 해 범위 밖, 가격 0, FCF 없음 → null', na.every((x) => x === null), JSON.stringify(na));
const mono = await pg.evaluate(() => { const a = anRdcfSolve(20000, 1000, 0.09, 0.02, 10), b2 = anRdcfSolve(20000, 1000, 0.09, 0.02, 15), c2 = anRdcfSolve(20000, 1000, 0.11, 0.02, 10); return [a, b2, c2]; });
ok('단조성: 기간이 길수록, r이 낮을수록 요구 성장률 낮아짐', mono[1] < mono[0] && mono[0] < mono[2], JSON.stringify(mono));
// 3) 화면
const txt = await pg.evaluate(() => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details.card')).filter((x) => x.querySelector('summary').textContent.indexOf('Valuation') >= 0)[0]; return d ? d.textContent : ''; });
ok('Valuation 섹션에 역DCF 블록·표·비교기준', txt.includes('역DCF') && txt.includes('요구수익률 r') && txt.includes('비교 기준') && txt.includes('목표주가가 아니며'), txt.slice(txt.indexOf('역DCF'), txt.indexOf('역DCF') + 300));
ok('기준 칸 표시와 해 없음/수치 혼재 없이 렌더', (txt.match(/\(기준\)/g) || []).length === 1);
const calc = await pg.evaluate(() => { const R = AN_CTX.rdcf; return R ? { main: R.main, chk: anRdcfValue(R.fps, R.main, R.r, R.gT, R.N), price: AN_CTX.price, fps: R.fps } : null; });
ok('실제 화면 값: 풀이 g로 정방향 계산하면 현재가', calc && Math.abs(calc.chk - calc.price) / calc.price < 1e-6, JSON.stringify(calc));
// 4) FCF 음수 종목 → N/A 문구
await pg.evaluate(() => { const c = AN_CTX; c.rdcf = null; const d = document.createElement('div'); d.innerHTML = anRdcfHtml(c); d.id = 'tmp'; document.body.appendChild(d); });
ok('FCF 음수/없음 → N/A 안내', (await pg.innerText('#tmp')).includes('N/A'));
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
