// 사용법: node tests/ui_fintbl_test.mjs (Playwright + Chromium 필요)
// 재무 표 탭: 가짜 API(종목 6개, 시세 요약)로 값을 손계산과 비교하고, 열 정렬·N/A 맨 아래·열 묶음 숨기기·필터·금융업 처리·TTM 결측·CSV를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uift_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
// ---- 모의 재무: 연간 합계 R(y)=1000억×(1+g)^(y-2019), 분기 손익은 연간÷4(분기 단독값), 현금흐름은 누적(YTD)
const RC = { 1: '11013', 2: '11012', 3: '11014' };
function mk(code, cfg) {
  const R = (y) => 1000e8 * Math.pow(1 + cfg.g, y - 2019), opm = (y) => cfg.opm0 + cfg.opmStep * (y - 2019), nim = (y) => cfg.nim0 + (cfg.nimStep || 0) * (y - 2019), eq = (y) => 3000e8 * Math.pow(1.1, y - 2019);
  const base = (y, rc, lab, po, f, cf) => ({ corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS',
    revenue: R(y) * f, cogs: R(y) * f * 0.6, operating_income: R(y) * opm(y) * f, net_income: R(y) * nim(y) * f, parent_net_income: R(y) * nim(y) * f,
    ocf: R(y) * 0.12 * cf, capex: R(y) * 0.04 * cf, fcf: R(y) * 0.08 * cf, total_equity: eq(y), parent_equity: eq(y), total_liabilities: 1500e8, cash: 300e8, total_shares: 1e7, treasury_shares: 0,
    filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, price_at_period_end: 20000, receivables: 150e8, inventory: 100e8, payables: 100e8, tangible_assets: 600e8, current_assets: 900e8, current_liabilities: 500e8 });
  const rows = [];
  for (let y = 2019; y <= 2024; y++) {
    if (!cfg.noQ) for (const q of [1, 2, 3]) rows.push(base(y, RC[q], q + '분기', q, 0.25, q / 4));
    rows.push(base(y, '11011', '사업보고서', 4, 1, 1));
  }
  if (cfg.q25) for (const q of cfg.q25) rows.push(base(2025, RC[q], q + '분기', q, 0.25, q / 4));
  return rows;
}
const CFG = {
  A: { g: 0.10, opm0: 0.10, opmStep: 0.01, nim0: 0.08, q25: [1, 2] },
  B: { g: -0.05, opm0: 0.10, opmStep: -0.024, nim0: 0.08, nimStep: -0.022, q25: [1, 2] }, // 영업이익률 10%→-2%(2024), 순이익률 8%→-3%
  C: { g: 0.15, opm0: 0.10, opmStep: 0.01, nim0: 0.08, noQ: true },
  D: { g: 0.05, opm0: 0.10, opmStep: 0.01, nim0: 0.08, q25: [1, 2] },
  E: { g: 0.02, opm0: 0.10, opmStep: 0.01, nim0: 0.08, noQ: true, q25: [1] }, // 2025 1분기만 있고 2024 분기 자료가 없어 TTM 불가
  F: { g: 0.20, opm0: 0.10, opmStep: 0.01, nim0: 0.08, q25: [1, 2] },
};
const COMP = [['A', '우량전자', '000001', '26110'], ['B', '부진화학', '000002', '20110'], ['C', '무시세물산', '000003', '27110'], ['D', '모의은행', '000004', '64110'], ['E', '결측TTM', '000005', '10110'], ['F', '급변주', '000006', '26120']]
  .map((x) => ({ corp_code: x[0], corp_name: x[1], stock_code: x[2], ind: x[3], n: 1, acc_mt: 12 }));
const DATA = {}; Object.keys(CFG).forEach((k) => { DATA[k] = mk(k, CFG[k]); });
const D0 = '20261113';
// [마지막 거래일, 종가, 52주 고가, 52주 저가, 거래일 수, 거래량, 거래대금, 20일 평균 거래대금, 시가총액, 불연속]
const SNAP = { rows: {
  '000001': [D0, 24000, 30000, 18000, 250, 123456, 5e9, 4e9, 3e12, 0],
  '000002': [D0, 20000, 20000, 10000, 250, 1000, 1e9, 2e9, null, 0],
  '000004': [D0, 50000, 60000, 40000, 250, 90000, 3e10, 2.5e10, 8e12, 0],
  '000005': ['20261106', 1000, 1500, 500, 150, 10, 1e7, 1e7, null, 0],
  '000006': [D0, 30000, 45000, 20000, 250, 5000, 2e9, 2e9, 5e11, 1],
}, date: D0 };
let snapCalls = [];
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0], u = new URL(q.url, 'http://x');
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [] }));
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: COMP }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => DATA[c] || [])) })); }
  if (p === '/api/fin/market-snap') { snapCalls.push(q.url); return r.end(JSON.stringify(SNAP)); }
  if (p === '/api/data/fresh') return r.end('{"market_last":"' + D0 + '","screen_last":null}');
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const near = (a, e, t) => typeof a === 'number' && Math.abs(a - e) <= Math.abs(e) * (t || 1e-9) + 1e-12;
const errs = [];
const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, acceptDownloads: true });
const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(500);
await pg.click('#bn_find'); await pg.click('#tabBtnFt'); await pg.waitForTimeout(200);
ok('발굴 묶음에 「재무 표」 메뉴, 화면 열림', await pg.isVisible('#tabFt') && await pg.isVisible('#tabBtnFt') && !(await pg.isVisible('#tabFscr')));
await pg.click('#tabFt button:has-text("저장된 종목 불러오기·계산")'); await pg.waitForTimeout(1500);
const names = () => pg.$$eval('#ftTbl tbody tr td.an-first a', (e) => e.map((x) => x.textContent.trim()));
ok('6개 종목이 표에 나옴, 시세 요약 1회 호출', (await names()).length === 6 && snapCalls.length === 1, (await names()).join());
ok('기본 정렬 = 시가총액 큰 순, 시총을 못 구한 종목은 맨 아래', (await names()).join() === '모의은행,우량전자,급변주,부진화학,결측TTM,무시세물산', (await names()).join());
// ---- 값 손계산(앱 함수가 아니라 상수로 직접 계산)
const R = (y) => 1000e8 * Math.pow(1.1, y - 2019), opm = (y) => 0.10 + 0.01 * (y - 2019), eq = (y) => 3000e8 * Math.pow(1.1, y - 2019);
const ttmRev = (R(2024) + R(2025)) / 2, ttmEbit = (opm(2024) * R(2024) + opm(2025) * R(2025)) / 2, ttmNi = 0.08 * ttmRev;
const prevTtmRev = (R(2023) + R(2024)) / 2, prevTtmEbit = (opm(2023) * R(2023) + opm(2024) * R(2024)) / 2;
const A = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '우량전자')[0]; const o = {}; Object.keys(r).forEach((k) => { if (typeof r[k] === 'number' || typeof r[k] === 'string' || r[k] === null || typeof r[k] === 'boolean') o[k] = r[k]; }); return o; });
ok('A 시세: 종가·52주 고가 대비 −20%·범위 위치 50%·시총·거래대금', A.price === 24000 && near(A.fromHi, 24000 / 30000 - 1) && near(A.rangePos, 0.5) && A.mcap === 3e12 && A.mcapDb === true && A.tv0 === 5e9 && A.tv20 === 4e9 && A.vol0 === 123456, JSON.stringify([A.price, A.fromHi, A.rangePos, A.mcap]));
ok('A 매출: 회기말·TTM·성장률(회기말·TTM)·3Y CAGR', near(A.revFY, R(2024)) && near(A.revTTM, ttmRev) && near(A.revGFY, 0.1, 1e-9) && near(A.revGTTM, ttmRev / prevTtmRev - 1) && near(A.revCagr, 0.1, 1e-9), [A.revFY, A.revTTM, A.revGFY, A.revGTTM, A.revCagr].join());
ok('A 영업이익·이익률(회기말·TTM)', near(A.ebitFY, 0.15 * R(2024)) && near(A.opmFY, 0.15) && near(A.ebitTTM, ttmEbit) && near(A.opmTTM, ttmEbit / ttmRev), [A.ebitFY, A.opmFY, A.ebitTTM, A.opmTTM].join());
ok('A 영업이익 성장률(회기말·TTM)·3Y CAGR·이익률 변화 +3%p', near(A.ebitGFY, 0.15 * R(2024) / (0.14 * R(2023)) - 1) && near(A.ebitGTTM, ttmEbit / prevTtmEbit - 1) && near(A.ebitCagr, Math.pow(0.15 * R(2024) / (0.12 * R(2021)), 1 / 3) - 1) && near(A.opmChg, 0.03), [A.ebitGFY, A.ebitGTTM, A.ebitCagr, A.opmChg].join());
ok('A EPS(회기말·TTM)·성장률·CAGR', near(A.epsFY, 0.08 * R(2024) / 1e7) && near(A.epsTTM, ttmNi / 1e7) && near(A.epsGFY, 0.1, 1e-9) && near(A.epsGTTM, 0.1, 1e-9) && near(A.epsCagr, 0.1, 1e-9), [A.epsFY, A.epsTTM, A.epsGFY, A.epsCagr].join());
ok('A PER·PBR (회기말: 사업보고서 값 / TTM: TTM·최근 분기말 값, 시총 3조)', near(A.perFY, 3e12 / (0.08 * R(2024))) && near(A.perTTM, 3e12 / ttmNi) && near(A.pbrFY, 3e12 / eq(2024)) && near(A.pbrTTM, 3e12 / eq(2025)), [A.perFY, A.perTTM, A.pbrFY, A.pbrTTM].join());
ok('A 부채비율·FCF·FCF 마진 = TTM 우선 기준(최근 분기말 부채/자본, TTM FCF)', near(A.deR, 1500e8 / eq(2025)) && near(A.fcfR, 0.08 * ttmRev) && near(A.fcfmR, 0.08, 1e-9) && A.basisTxt === 'TTM 2025 2Q', [A.deR, A.fcfR, A.fcfmR, A.basisTxt].join());
ok('A 재무 기준 표기', A.fyTxt === '2024 연간' && A.ttmTxt === 'TTM 2025 2Q');
// ---- 화면에 보이는 글자
const cell = (name, label) => pg.evaluate(([n, l]) => { const ths = Array.prototype.map.call(document.querySelectorAll('#ftTbl thead th'), (t) => t.getAttribute('data-k')); const col = FT_COLS.findIndex((c) => c.l === l); const key = FT_COLS[col].k; const idx = ths.indexOf(key); const tr = Array.prototype.filter.call(document.querySelectorAll('#ftTbl tbody tr'), (r) => r.querySelector('td.an-first a').textContent.trim() === n)[0]; return tr.children[idx].textContent.trim(); }, [name, label]);
ok('화면 표기: 종가 24,000 · 52주 고가 대비 -20.0% · 위치 50% · 시총 3.00조', await cell('우량전자', '종가') === '24,000' && await cell('우량전자', '52주 고가 대비') === '-20.0%' && await cell('우량전자', '52주 범위 내 위치') === '50%' && await cell('우량전자', '시가총액') === '3.00조');
ok('화면 표기: 매출액(회기말) 억 단위, 성장률 10.0%, EPS 원, PER 배', (await cell('우량전자', '매출액(회기말)')).endsWith('억') && await cell('우량전자', '매출 성장률(회기말)') === '10.0%' && await cell('우량전자', 'EPS(회기말)') === Math.round(0.08 * R(2024) / 1e7).toLocaleString() && (await cell('우량전자', 'PER(회기말)')).endsWith('배'));
// ---- N/A 처리
const B = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '부진화학')[0]; return { mcap: r.mcap, db: r.mcapDb, per: r.perFY, perT: r.perTTM, ebitC: r.ebitCagr, opmFY: r.opmFY, epsCagr: r.epsCagr, hi: r.fromHi, pos: r.rangePos, ebitG: r.ebitGFY }; });
ok('B 시총: DB 값 없으면 종가×주식수(2천억)로 대신하고 * 표시', B.mcap === 20000 * 1e7 && B.db === false && (await cell('부진화학', '시가총액')) === '2,000억*', await cell('부진화학', '시가총액'));
ok('B 적자: PER N/A, CAGR N/A(끝값 ≤ 0), 영업이익률 음수', B.per === null && B.perT === null && B.ebitC === null && B.opmFY < 0 && B.epsCagr === null && (await cell('부진화학', 'PER(회기말)')) === 'N/A', JSON.stringify(B));
ok('B 신고가: 52주 고가 대비 0.0%, 위치 100%', near(B.hi, 0) && near(B.pos, 1));
const C = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '무시세물산')[0]; return { price: r.price, mcap: r.mcap, per: r.perFY, fromHi: r.fromHi, hiNote: r.hiNote, revFY: r.revFY, revTTM: r.revTTM, ttmTxt: r.ttmTxt, tv0: r.tv0 }; });
ok('C 시세 없음: 종가·시총·PER·52주 모두 N/A(만들지 않음), 재무는 정상', C.price === null && C.mcap === null && C.per === null && C.fromHi === null && C.hiNote === '시세 없음' && C.tv0 === null && C.revFY > 0, JSON.stringify(C));
ok('C 최근 보고서가 사업보고서뿐이면 TTM = 회기말 (표기 「(=TTM)」)', C.revTTM === C.revFY && C.ttmTxt.indexOf('(=TTM)') > 0, C.ttmTxt);
const E = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '결측TTM')[0]; return { ttmTxt: r.ttmTxt, revTTM: r.revTTM, perTTM: r.perTTM, epsTTM: r.epsTTM, perFY: r.perFY, hi: r.fromHi, note: r.hiNote, stale: r.stale, basis: r.basisTxt, revG: r.revGTTM }; });
ok('E 더 새로운 분기가 있는데 TTM을 못 만들면 N/A(옛 연간값을 TTM인 척 쓰지 않음)', E.ttmTxt === 'TTM 산출 불가' && E.revTTM === null && E.perTTM === null && E.epsTTM === null && E.revG === null && E.perFY > 0, JSON.stringify(E));
ok('E 이력 부족(150거래일): 52주 항목 N/A + 마지막 거래일이 다르면 ⚠', E.hi === null && E.note.indexOf('이력 부족') >= 0 && E.stale === true && (await cell('결측TTM', '종가')).endsWith('⚠'), E.note);
ok('E TTM 못 만들면 ROE 등 최신 기준은 연간으로 대체되고 기준이 표시됨', E.basis === '2024 연간', E.basis);
const F = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '급변주')[0]; return { hi: r.fromHi, pos: r.rangePos, note: r.hiNote, price: r.price }; });
ok('F 가격 불연속(±30%/일) 의심: 52주 항목 N/A, 종가는 표시', F.hi === null && F.pos === null && F.note.indexOf('불연속') >= 0 && F.price === 30000, JSON.stringify(F));
// ---- 금융업
const D = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '모의은행')[0]; return { fin: r.fin, roic: r.roic, fcf: r.fcf, fcfm: r.fcfm, de: r.de, roe: r.roe, per: r.perFY }; });
ok('금융업(KSIC 64): ROIC·FCF·FCF 마진·부채비율 N/A, ROE·PER은 표시, 「금융」 표시', D.fin === true && D.roic === null && D.fcf === null && D.fcfm === null && D.de === null && D.roe != null && D.per > 0 && (await pg.innerText('#ftTbl tbody tr:first-child td.an-first')).includes('금융'), JSON.stringify(D));
const AR = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '우량전자')[0]; return { roic: r.roic, fcf: r.fcf, de: r.de }; });
ok('일반 기업은 ROIC·FCF·부채비율 표시', AR.roic != null && AR.fcf != null && AR.de != null, JSON.stringify(AR));
// ---- 정렬: 열 제목 클릭, 다시 클릭하면 반대, N/A는 어느 방향이든 맨 아래
await pg.click('#ftTbl thead th[data-k="revGFY"]'); await pg.waitForTimeout(100);
ok('매출 성장률(회기말) 내림차순: 급변주20% > 무시세15% > 우량10% > 은행5% > 결측TTM2% > 부진-5%', (await names()).join() === '급변주,무시세물산,우량전자,모의은행,결측TTM,부진화학', (await names()).join());
ok('정렬 방향 표시 ▼', (await pg.innerText('#ftTbl thead th[data-k="revGFY"]')).includes('▼'));
await pg.click('#ftTbl thead th[data-k="revGFY"]'); await pg.waitForTimeout(100);
ok('같은 열 다시 클릭 → 오름차순', (await names()).join() === '부진화학,결측TTM,모의은행,우량전자,무시세물산,급변주' && (await pg.innerText('#ftTbl thead th[data-k="revGFY"]')).includes('▲'), (await names()).join());
await pg.click('#ftTbl thead th[data-k="price"]'); await pg.waitForTimeout(100);
ok('종가 내림차순(처음 클릭은 큰 값부터), 종가 없는 종목은 맨 아래', (await names()).join() === '모의은행,급변주,우량전자,부진화학,결측TTM,무시세물산', (await names()).join());
await pg.click('#ftTbl thead th[data-k="price"]'); await pg.waitForTimeout(100);
ok('종가 오름차순에서도 종가 없는 종목은 여전히 맨 아래', (await names()).join() === '결측TTM,부진화학,우량전자,급변주,모의은행,무시세물산', (await names()).join());
await pg.click('#ftTbl thead th[data-k="perFY"]'); await pg.waitForTimeout(100);
const perOrder = await names();
ok('PER(회기말)은 낮은 값부터(처음 클릭 = 오름차순), PER N/A(적자·시세없음)는 맨 아래', perOrder.slice(-2).sort().join() === '무시세물산,부진화학' && perOrder.indexOf('모의은행') < perOrder.indexOf('우량전자') + 6, perOrder.join());
await pg.click('#ftTbl thead th[data-k="de"]'); await pg.waitForTimeout(100);
ok('부채비율 정렬: 금융업(N/A)은 맨 아래쪽', (await names()).indexOf('모의은행') >= 4, (await names()).join());
await pg.click('#ftTbl thead th[data-k="name"]'); await pg.waitForTimeout(100);
const byName = await names(); ok('종목명 정렬(가나다)', byName.join() === byName.slice().sort((x, y) => x.localeCompare(y, 'ko')).join(), byName.join());
// ---- 열 묶음 숨기기
const hasTh = (k) => pg.$('#ftTbl thead th[data-k="' + k + '"]').then((e) => !!e);
await pg.uncheck('#ftGroups input[data-g="eps"]'); await pg.waitForTimeout(100);
ok('EPS 묶음을 끄면 EPS 열이 사라지고 다른 열은 유지', !(await hasTh('epsFY')) && !(await hasTh('epsCagr')) && await hasTh('revFY') && await hasTh('perFY'));
await pg.check('#ftGroups input[data-g="eps"]'); await pg.waitForTimeout(100);
ok('다시 켜면 EPS 열 복귀', await hasTh('epsFY'));
const allCols = await pg.$$eval('#ftTbl thead th', (e) => e.length);
const need = ['종가', '52주 고가 대비', '시가총액', '거래대금(최근일)', '매출액(회기말)', '매출액(TTM)', '매출 성장률(회기말)', '매출 성장률(TTM)', '매출 CAGR', '영업이익(회기말)', '영업이익(TTM)', '영업이익률(회기말)', '영업이익률(TTM)', '영업이익 성장률(회기말)', '영업이익 성장률(TTM)', '영업이익 CAGR', 'EPS(회기말)', 'EPS(TTM)', 'EPS 성장률(회기말)', 'EPS 성장률(TTM)', 'EPS CAGR', 'ROE', 'ROIC', 'FCF', 'FCF 마진', '부채비율', 'PER(회기말)', 'PER(TTM)', 'PBR(회기말)', 'PBR(TTM)'];
const labels = await pg.evaluate(() => FT_COLS.map((c) => c.l));
ok('요청한 항목(종가·52주·시총·거래대금·매출·영업이익·EPS·ROE·ROIC·FCF·부채비율·PER·PBR)이 모두 열로 있음', need.every((n) => labels.indexOf(n) >= 0) && allCols === 3 + labels.length, 'columns=' + allCols + ' missing=' + need.filter((n) => labels.indexOf(n) < 0).join());
// ---- 필터
await pg.fill('#ftQ', '우량'); await pg.waitForTimeout(100);
ok('검색: 종목명 일부로 거르기', (await names()).join() === '우량전자');
await pg.fill('#ftQ', '000004'); await pg.waitForTimeout(100);
ok('검색: 종목코드로도 거르기', (await names()).join() === '모의은행');
await pg.fill('#ftQ', ''); await pg.selectOption('#ftSec', 'fin'); await pg.waitForTimeout(100);
ok('유형: 금융업만', (await names()).join() === '모의은행');
await pg.selectOption('#ftSec', 'gen'); await pg.waitForTimeout(100);
ok('유형: 일반 기업(금융 제외) 5개', (await names()).length === 5 && !(await names()).includes('모의은행'));
await pg.selectOption('#ftSec', ''); await pg.fill('#ftMinCap', '5000'); await pg.dispatchEvent('#ftMinCap', 'change'); await pg.waitForTimeout(100);
ok('시가총액 ≥ 5,000억: 은행(8조)·우량(3조)·급변(5,000억)만', (await names()).sort().join() === ['급변주', '모의은행', '우량전자'].sort().join(), (await names()).join());
await pg.fill('#ftMinCap', '0'); await pg.dispatchEvent('#ftMinCap', 'change'); await pg.check('#ftHideNoPx'); await pg.waitForTimeout(100);
ok('시세 없는 종목 숨기기', (await names()).length === 5 && !(await names()).includes('무시세물산'));
await pg.uncheck('#ftHideNoPx');
await pg.selectOption('#ftInd', '26'); await pg.waitForTimeout(100);
ok('업종(KSIC 26) 필터', (await names()).sort().join() === ['급변주', '우량전자'].sort().join(), (await names()).join());
await pg.selectOption('#ftInd', '');
// ---- 기간·기준 선택
await pg.selectOption('#ftN', '5'); await pg.waitForTimeout(200);
const A5 = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '우량전자')[0]; return { c: r.revCagr, m: r.opmChg, e: r.ebitCagr }; });
ok('CAGR·마진 변화 5년으로 바꾸면 2019년 기준: 이익률 +5%p, 영업이익 CAGR 손계산 일치', near(A5.c, 0.1, 1e-9) && near(A5.m, 0.05) && near(A5.e, Math.pow(0.15 * R(2024) / (0.10 * R(2019)), 1 / 5) - 1), JSON.stringify(A5));
await pg.selectOption('#ftBasis', 'annual'); await pg.waitForTimeout(200);
const Aa = await pg.evaluate(() => { const r = FT.rows.filter((x) => x.name === '우량전자')[0]; return { b: r.basisTxt, de: r.deR, fcf: r.fcfR, rev: r.revTTM }; });
ok('ROE·FCF 기준을 연간으로: 2024 연간 값(부채비율=1500억÷2024 자본), 매출 TTM 열은 그대로', Aa.b === '2024 연간' && near(Aa.de, 1500e8 / eq(2024)) && near(Aa.fcf, 0.08 * R(2024)) && near(Aa.rev, ttmRev), JSON.stringify(Aa));
await pg.selectOption('#ftN', '3'); await pg.selectOption('#ftBasis', 'ttm'); await pg.waitForTimeout(200);
ok('선택을 바꿔도 서버 시세 요약은 다시 받지 않음', snapCalls.length === 1);
await pg.click('#tabFt button:has-text("시세 요약 다시 계산")'); await pg.waitForTimeout(600);
ok('「시세 요약 다시 계산」은 fresh=1로 서버 호출', snapCalls.length === 2 && snapCalls[1].indexOf('fresh=1') > 0, snapCalls.join(' '));
// ---- CSV
const [dl] = await Promise.all([pg.waitForEvent('download'), pg.click('#tabFt button:has-text("CSV 저장")')]);
const csv = fs.readFileSync(await dl.path(), 'utf8');
const lines = csv.replace(/^﻿/, '').split('\n');
ok('CSV: 머리글에 요청 항목, 종목 6행 + 설명 1행, BOM', csv.charCodeAt(0) === 0xFEFF && lines[0].includes('"52주 고가 대비"') && lines[0].includes('"EPS 성장률(TTM)"') && lines.length === 8 && dl.suggestedFilename() === 'fin_table.csv', lines.length + ' lines');
const aLine = lines.filter((l) => l.startsWith('"우량전자"'))[0];
ok('CSV: 우량전자 값이 원 단위 숫자로 들어감(종가 24000, 시총 3e12)', aLine && aLine.includes('"24000"') && aLine.includes('"3000000000000"'), (aLine || '').slice(0, 120));
const bank = lines.filter((l) => l.startsWith('"모의은행"'))[0];
ok('CSV: 금융업 ROIC·FCF 칸은 비어 있음', bank && (bank.match(/""/g) || []).length >= 4);
// ---- 모양
const overflow = await pg.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }));
ok('아이폰 폭(390)에서 페이지 가로 넘침 없음(표는 안쪽에서 스크롤)', overflow.sw <= overflow.cw, JSON.stringify(overflow));
await pg.evaluate(() => window.scrollTo(0, 0));
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_fintbl.png') });
ok('JS 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
