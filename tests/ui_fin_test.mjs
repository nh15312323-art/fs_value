// 사용법: node tests/ui_fin_test.mjs (Playwright + Chromium 필요)
// 금융업·지주회사: 분류 규칙, 정당 PBR·NAV 손계산, 은행/보험/금융지주/지주/일반 종목 화면, 화면 전환 시 잔상, 스크리닝 유형 선택, 백테스트 대상 선택.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uifin_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const dates = []; { let d = new Date(Date.UTC(2024, 9, 2)); while (dates.length < 400) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round(60000 + i * 20 + 2000 * Math.sin(i / 20)));
const PERIODS = [['11013', '1분기', 1], ['11012', '반기', 2], ['11014', '3분기', 3], ['11011', '사업보고서', 4]];
let seed = 3; const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
const bankRows = (code, k) => { const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of PERIODS) {
  const g = 1 + (y - 2018) * 0.05, q = po / 4, eq = 40e12 * g * k, ni = 4e12 * g * q * k;
  rows.push({ corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: null, cogs: null, operating_income: null, net_income: ni, parent_net_income: ni * 0.98, total_equity: eq, parent_equity: eq * 0.97, total_liabilities: eq * 11, cash: 8e12 * k, ocf: -3e12 * q * k, capex: 3e11 * q * k, fcf: null, total_shares: 4.2e8 * k, treasury_shares: 0, dividend_per_share: rc === '11011' ? 2500 : null, pretax_income: ni * 1.3, filing_date: (y + (rc === '11011' ? 1 : 0)) + '0320', price_at_period_end: 55000, price_next_day: 50000 + (y - 2018) * 3000 + Math.round(rnd() * 4000), price_next_day_date: (y + 1) + '0321', current_assets: null }); } return rows; };
const genRows = (code) => { const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of PERIODS) {
  const g = 1 + (y - 2018) * 0.1, q = po / 4;
  rows.push({ corp_code: code, bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g * q, cogs: 600e8 * g * q, operating_income: 150e8 * g * q, net_income: 110e8 * g * q, parent_net_income: 110e8 * g * q, total_equity: 800e8 * g, parent_equity: 800e8 * g, total_liabilities: 500e8 * g, cash: 100e8, ocf: 140e8 * g * q, capex: 40e8 * g * q, fcf: 100e8 * g * q, total_shares: 1e7, treasury_shares: 0, dividend_per_share: rc === '11011' ? 300 : null, pretax_income: 140e8 * g * q, current_assets: 400e8 * g, current_liabilities: 250e8 * g, depreciation: 30e8 * q, filing_date: (y + (rc === '11011' ? 1 : 0)) + '0320', price_at_period_end: 20000, price_next_day: 15000 + (y - 2018) * 2500 + Math.round(rnd() * 3000), price_next_day_date: (y + 1) + '0321' }); } return rows; };
const COMP = [
  { corp_code: 'B', corp_name: '모의은행', stock_code: '000002', ind: '64121', n: 28, acc_mt: 12 },
  { corp_code: 'G', corp_name: '모의전자', stock_code: '000001', ind: '26110', n: 28, acc_mt: 12 },
  { corp_code: 'H', corp_name: '모의지주', stock_code: '000003', ind: '64992', n: 28, acc_mt: 12 },
  { corp_code: 'I', corp_name: '모의생명', stock_code: '000004', ind: '', n: 28, acc_mt: 12 },
  { corp_code: 'F', corp_name: '모의금융지주', stock_code: '000005', ind: '64992', n: 28, acc_mt: 12 },
];
const DATA = { B: bankRows('B', 1), G: genRows('G'), H: genRows('H'), I: bankRows('I', 0.3), F: bankRows('F', 0.8) };
const byName = {}; COMP.forEach((c) => { byName[c.corp_name] = c; });
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: COMP.map((c) => ({ n: c.corp_name, c: c.stock_code })) }));
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: COMP }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => DATA[c] || [])) })); }
  if (p === '/api/fin/last-price') return r.end(JSON.stringify({ date: '20261023', prices: { '000001': 20000, '000002': 55000, '000003': 20000, '000004': 55000, '000005': 55000 } }));
  if (p === '/api/market/caps') { globalThis.__capsAsked = u.searchParams.get('codes'); return r.end(JSON.stringify({ date: '20261023', caps: { '000001': { cap: 2e12, close: 20000 } } })); }
  if (p.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: 'x', stock_code: '000002', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: close.map(() => 1e6), tv: close.map((c) => c * 1e6) }));
  if (p.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: 'x', close: close[close.length - 1], date: dates[dates.length - 1] }));
  if (p.startsWith('/api/financial-history-db')) { const c = byName[u.searchParams.get('corp_name')]; if (!c) { r.statusCode = 404; return r.end('{"error":"none"}'); } return r.end(JSON.stringify({ corp_name: c.corp_name, corp_code: c.corp_code, stock_code: c.stock_code, induty_code: c.ind || null, acc_mt: 12, rows: DATA[c.corp_code] })); }
  if (p.startsWith('/api/industry/')) return r.end('{"map":{}}');
  r.statusCode = 404; r.end('{"error":"none"}');
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e))); pg.on('console', (m) => { if (m.type() === 'error' && /화면 그리기 오류|표시 오류/.test(m.text())) errs.push('CONSOLE ' + m.text().slice(0, 200)); });
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(500);

// 1) 분류 규칙
const cases = [
  ['64121', '모의은행', 'bank'], ['', '카카오뱅크', 'bank'], ['64992', '신한지주', 'finhold'], ['64992', 'KB금융', 'finhold'], ['64992', '한국금융지주', 'finhold'], ['64992', 'SK', 'holding'], ['64992', '롯데지주', 'holding'],
  ['65110', '삼성생명', 'insurer'], ['65121', 'DB손해보험', 'insurer'], ['', '삼성화재', 'insurer'], ['', '현대해상', 'insurer'], ['66121', '미래에셋증권', 'broker'], ['', '키움증권', 'broker'], ['66199', '한국거래소관련', 'finsvc'],
  ['64913', '삼성카드', 'credit'], ['64919', '아무캐피탈', 'credit'], ['64999', '아무인베스트먼트', 'invest'], ['64209', '무슨집합투자', 'invest'], ['', '대신밸런스제1호스팩', 'spac'], ['64999', '어떤스팩', 'spac'],
  ['26110', '삼성전자', null], ['', '삼성전자', null], ['', '녹십자생명과학', null], ['21101', '한미약품', null], ['', '', null], [null, undefined, null],
];
const got = await pg.evaluate((cs) => cs.map((c) => { const s = finSectorOf(c[0], c[1]); return s ? s.k : null; }), cases);
const badC = cases.filter((c, i) => got[i] !== c[2]).map((c, i) => c.join('/') + '→' + got[cases.indexOf(c)]);
ok('분류 규칙 ' + cases.length + '건 (코드 우선, 이름 보조, 비금융은 null)', badC.length === 0, badC.join(' | '));
const basis = await pg.evaluate(() => [finSectorOf('64121', '아무개').basis, finSectorOf('', '삼성화재').basis, finSectorOf('64992', 'SK').fin]);
ok('근거 표시: 코드 분류는 KSIC, 이름 분류는 이름 추정, 비금융 지주는 fin=false', basis[0] === 'KSIC 64121' && basis[1] === '이름 추정' && basis[2] === false, JSON.stringify(basis));

// 2) 수식 손계산
const m = await pg.evaluate(() => ({
  j: finJustPbr(0.12, 0.02, 0.09), jEq: finJustPbr(0.09, 0.02, 0.09), jLow: finJustPbr(0.01, 0.02, 0.09), jBad: finJustPbr(0.12, 0.09, 0.09), jBad2: finJustPbr(0.12, 0.1, 0.09), jNull: finJustPbr(null, 0.02, 0.09),
  imp: finImpliedRoe(0.5, 0.02, 0.09), rt: finImpliedRoe(finJustPbr(0.12, 0.02, 0.09), 0.02, 0.09), impBad: finImpliedRoe(-1, 0.02, 0.09), ddm: finDdm(1000, 0.02, 0.09), ddm0: finDdm(0, 0.02, 0.09),
  nav: finNav([{ stake: 50, cap: 1000 }, { stake: 30, cap: 2000 }, { stake: 20, cap: '', name: '미입력' }, { stake: 0, cap: 500, name: '지분없음' }, { stake: 150, cap: 500, name: '잘못된지분' }], 500, 200, 7e10, 1e7),
  nav2: finNav([], 0, 0, 1e11, 1e7), nav3: finNav([{ stake: 10, cap: 100 }], 0, 50, 1e10, 0),
}));
ok('정당 PBR (0.12−0.02)/(0.09−0.02)=1.4286, ROE=ke면 1', Math.abs(m.j - 10 / 7) < 1e-12 && Math.abs(m.jEq - 1) < 1e-12, m.j + ' ' + m.jEq);
ok('ROE<g면 0으로 바닥, ke≤g·입력 없음이면 N/A', m.jLow === 0 && m.jBad === null && m.jBad2 === null && m.jNull === null);
ok('시장이 요구하는 ROE = g + PBR(ke−g) = 0.02+0.5×0.07 = 0.055, 정당 PBR과 왕복 일치, 음수 PBR은 N/A', Math.abs(m.imp - 0.055) < 1e-12 && Math.abs(m.rt - 0.12) < 1e-12 && m.impBad === null, m.imp + ' ' + m.rt);
ok('배당할인 1000×1.02/0.07 = 14571.43, 배당 0이면 N/A', Math.abs(m.ddm - 1000 * 1.02 / 0.07) < 1e-9 && m.ddm0 === null);
ok('NAV: 0.5×1000 + 0.3×2000 + 기타 500 − 순차입 200 = 1400억, 반영 2개, 빠진 줄 3개(시총·지분없음·지분>100)', Math.abs(m.nav.nav - 1400) < 1e-9 && m.nav.used === 2 && m.nav.miss.length === 3, JSON.stringify(m.nav));
ok('NAV 할인율 = 1 − 700억/1400억 = 0.5, 주당 NAV = 1400억/1000만주 = 14,000원', Math.abs(m.nav.discount - 0.5) < 1e-12 && Math.abs(m.nav.navPerShare - 14000) < 1e-6, m.nav.discount + ' ' + m.nav.navPerShare);
ok('NAV가 0 이하이면 할인율 N/A, 주식수 없으면 주당 NAV N/A', m.nav2.discount === null && m.nav3.navPerShare === null && m.nav3.discount === null, JSON.stringify([m.nav2, m.nav3]));

const openStock = async (name) => { await pg.evaluate(() => switchTab('financial')); await pg.fill('#corpName', name); await pg.keyboard.press('Enter'); await pg.waitForTimeout(2300); };
const aTxt = async () => pg.evaluate(() => document.getElementById('analysis').innerText);
const sTxt = async () => pg.evaluate(() => document.getElementById('summary').innerText);

// 3) 은행
await openStock('모의은행');
let t = await aTxt();
const chk = { bank: t.includes('은행'), j: t.includes('정당 PBR'), pr: t.includes('PBR과 ROE의 관계'), miss: t.includes('수집하지 않는 지표'), noR: !t.includes('ROIC 4단계'), noF: !t.includes('FCF 유형'), noC: !t.includes('종합 분류') };
ok('은행: 금융업 전용 화면(한눈에 보기·정당 PBR·미수집 지표), 일반 기업용 장(ROIC 4단계·FCF 유형·종합 분류) 없음', Object.values(chk).every(Boolean), JSON.stringify(chk));
const fb = await pg.evaluate(() => { const o = AN_CTX.fin; const roes = AN_CTX.list.map((d) => d.roe).filter(anFin); const k = o.ke, g = o.g; return { roe3: o.roe3, hand3: roes.slice(-3).reduce((a, v) => a + v, 0) / 3, base: o.roeBase, j: o.jPbr, jh: (o.roeBase - g) / (k - g), pbr: o.pbr, imp: o.impRoe, back: finJustPbr(o.impRoe, g, k), gridC: o.grid.rows[2].cells[1], n: o.rows.length, jp: o.jPrice, bps: o.bps, ke: k }; });
ok('은행: ROE 3Y평균 손계산 일치', Math.abs(fb.roe3 - fb.hand3) < 1e-12, fb.roe3 + ' ' + fb.hand3);
ok('은행: 정당 PBR = (ROE−g)/(ke−g) 손계산, 민감도 표 가운데 칸과 같음, 시장 요구 ROE 왕복', Math.abs(fb.j - fb.jh) < 1e-12 && Math.abs(fb.gridC - fb.j) < 1e-12 && Math.abs(fb.back - fb.pbr) < 1e-9, JSON.stringify(fb));
ok('은행: 정당 PBR 환산 주가 = 정당 PBR × BPS', Math.abs(fb.jp - fb.j * fb.bps) < 1e-6 && fb.n === 7);
const sm = await sTxt();
ok('은행 요약 카드: 부채비율·이자보상배율·매출총이익 항목 행과 "주의" 배지가 없고 자기자본승수가 있음', sm.includes('자기자본승수') && !sm.includes('부채비율(부채/자본)') && !sm.includes('이자보상배율 (') && !sm.includes('매출총이익/총자산') && /ROE 평균 \(7개년\)\s*[\d.]+%\s*자기자본승수[^\n]*\n\s*[\d.]+배\s*\n/.test(sm), (sm.match(/수익성·자본[\s\S]{0,260}/) || [''])[0].replace(/\n/g, ' '));
ok('은행: 재조회 필요(감가상각·차입금) 경고 없음', !t.includes('아직 저장되어 있지 않습니다'));
// 읽는 법 문장
const rd = await pg.evaluate(() => AN_CTX.fin.read); ok('은행: 읽는 법 분기 값 존재(below/cheap/rich/fair)', ['below', 'cheap', 'rich', 'fair'].includes(rd), String(rd));
// 일반 화면 전환
await pg.click('#anGlance button:has-text("일반 기업용 화면으로 보기")'); await pg.waitForTimeout(500);
t = await aTxt();
ok('은행: 「일반 기업용 화면」으로 바꾸면 ROIC 4단계 등이 보이고 안내 배너 표시', t.includes('ROIC 4단계') && t.includes('일반 기업용 지표 화면으로 보는 중') && !t.includes('PBR과 ROE의 관계'));
await pg.click('#analysis button:has-text("자동 분류로 되돌리기")'); await pg.waitForTimeout(500);
t = await aTxt(); ok('은행: 자동 분류로 되돌리면 다시 금융업 화면', t.includes('PBR과 ROE의 관계') && !t.includes('ROIC 4단계'));

// 4) 일반 기업, 잔상 확인
await openStock('모의전자');
t = await aTxt();
ok('일반 기업: 기존 화면(ROIC 4단계·종합 분류) 그대로, 금융업 안내 없음', t.includes('ROIC 4단계') && t.includes('종합 분류') && !t.includes('PBR과 ROE의 관계') && !t.includes('NAV'));
const meta = await pg.evaluate(() => document.getElementById('anIndMeta') ? document.getElementById('anIndMeta').innerText : '');
ok('일반 기업: 업종 줄에 「금융업 화면으로 보기」 링크', meta.includes('금융업 화면으로 보기'), meta);
await pg.evaluate(() => document.querySelector('#anIndMeta a').click()); await pg.waitForTimeout(500);
t = await aTxt(); ok('직접 「금융업 화면」 선택 시 금융업 화면(직접 선택 표시)', t.includes('PBR과 ROE의 관계') && t.includes('직접 선택'));
await pg.click('#anGlance button:has-text("자동 분류로 되돌리기")'); await pg.waitForTimeout(500);
t = await aTxt(); ok('되돌리면 일반 화면', t.includes('ROIC 4단계') && !t.includes('PBR과 ROE의 관계'));
await openStock('모의은행'); t = await aTxt();
ok('일반→은행 다시 조회해도 금융업 화면(이전 종목 잔상 없음)', t.includes('PBR과 ROE의 관계') && !t.includes('ROIC 4단계'));

// 5) 보험(이름 추정), 금융지주
await openStock('모의생명'); t = await aTxt();
ok('이름만으로 보험 추정: 「보험」 분류, 근거 「이름 추정」 문구', t.includes('보험') && t.includes('이름 추정') && t.includes('K-ICS') && t.includes('CSM'));
await openStock('모의금융지주'); t = await aTxt();
ok('금융지주(코드 64992 + 이름): 금융지주로 분류', t.includes('금융지주') && t.includes('PBR과 ROE의 관계') && !t.includes('NAV 대비 할인율'));

// 6) 지주회사 NAV
await openStock('모의지주'); t = await aTxt();
ok('비금융 지주회사: 일반 화면 + NAV 계산기, 한눈에 보기 뒤에 놓임', t.includes('NAV 대비 할인율') && t.includes('ROIC 4단계') && t.indexOf('한눈에 보기') < t.indexOf('NAV 대비 할인율'));
await pg.fill('#finNavNm0', '자회사A'); await pg.fill('#finNavCd0', '000001'); await pg.fill('#finNavSt0', '50');
await pg.click('button:has-text("시가총액 자동 채우기")'); await pg.waitForTimeout(600);
ok('시총 자동 채우기: 서버에 종목코드 전달, 2조 원=20,000억 입력', globalThis.__capsAsked === '000001' && (await pg.inputValue('#finNavCap0')) === '20000', globalThis.__capsAsked + ' ' + (await pg.inputValue('#finNavCap0')));
await pg.fill('#finNavDebt', '1000'); await pg.fill('#finNavOther', '0'); await pg.waitForTimeout(200);
const nv = await pg.evaluate(() => { const m = lastSummaryMetrics, c = AN_CTX; const r = finNav(FIN_NAV.rows, FIN_NAV.other, FIN_NAV.debt, m.marketCap, c.last.shares); return { r, txt: document.getElementById('finNavOut').innerText, mcap: m.marketCap }; });
ok('NAV 계산: 0.5×20,000억 − 1,000억 = 9,000억', Math.abs(nv.r.nav - 9000) < 1e-9 && nv.txt.includes('9,000억'), nv.txt.replace(/\n/g, ' ').slice(0, 200));
ok('NAV 할인율 표시 = 1 − 시총/NAV (화면 값과 손계산 일치)', nv.txt.includes((((1 - nv.mcap / 1e8 / 9000) * 100).toFixed(1)) + '%'), (((1 - nv.mcap / 1e8 / 9000) * 100).toFixed(1)) + '% vs ' + nv.txt.replace(/\n/g, ' ').slice(0, 250));
// 다시 그려도 입력 유지, 다른 종목 갔다 와도 저장값 복원
await pg.evaluate(() => renderAnalysis()); await pg.waitForTimeout(300);
ok('다시 그려도 NAV 입력 유지', (await pg.inputValue('#finNavSt0')) === '50' && (await pg.inputValue('#finNavDebt')) === '1000');
await openStock('모의전자'); await openStock('모의지주');
ok('다른 종목을 보고 돌아와도 저장된 NAV 입력 복원(이 브라우저에 저장)', (await pg.inputValue('#finNavSt0')) === '50' && (await pg.inputValue('#finNavNm0')) === '자회사A');
await openStock('모의전자'); t = await aTxt(); ok('일반 종목에는 NAV 계산기가 나타나지 않음', !t.includes('NAV 대비 할인율'));

// 7) 재무 스크리닝
await pg.click('#bn_find'); await pg.click('#tabBtnFscr'); await pg.waitForTimeout(200);
ok('재무 스크리닝 조건 12개(ROE 3Y 추가)', (await pg.$$('#fscFilters input[type=checkbox]')).length === 12);
await pg.click('button:has-text("저장된 종목 불러오기")'); await pg.waitForTimeout(1800);
const names = async () => pg.$$eval('#fscTbl tbody tr[data-code] td.an-first', (e) => e.map((x) => x.textContent.replace('⚠', '').trim()).sort());
ok('유형 전체: 5종목', (await names()).length === 5, (await names()).join());
const sel = async (v) => { await pg.selectOption('#fscSec', v); await pg.waitForTimeout(250); return names(); };
ok('유형=일반 기업 → 모의전자만', (await sel('gen')).join() === '모의전자');
ok('유형=금융업 전체 → 은행·보험·금융지주 3개(지주회사 제외)', (await sel('fin')).join() === ['모의금융지주', '모의생명', '모의은행'].sort().join(), (await names()).join());
ok('유형=은행·금융지주 → 2개', (await sel('bank')).join() === ['모의금융지주', '모의은행'].sort().join());
ok('유형=보험 → 모의생명(이름 추정)', (await sel('insurer')).join() === '모의생명');
ok('유형=지주회사(비금융) → 모의지주', (await sel('holding')).join() === '모의지주');
const funnel = await pg.evaluate(() => document.querySelectorAll('#fscOut table')[0].innerText);
ok('단계표에 유형 단계가 기록됨', funnel.includes('유형: 지주회사'), funnel.replace(/\n/g, ' ').slice(0, 160));
await pg.click('button:has-text("금융업 ROE+PBR")'); await pg.waitForTimeout(300);
const st = await pg.evaluate(() => ({ sec: document.getElementById('fscSec').value, roe: document.getElementById('fscOn_roe3').checked, pbr: document.getElementById('fscOn_pbr').checked, roic: document.getElementById('fscOn_roic3').checked }));
ok('프리셋 「금융업 ROE+PBR」: 유형=금융업, ROE·PBR 조건만 켜짐', st.sec === 'fin' && st.roe && st.pbr && !st.roic, JSON.stringify(st));
const vals = await pg.evaluate(() => FSC.rows.filter((x) => x.name === '모의은행').map((x) => ({ roe3: x.roe3, hand: anMean(anWin(x.list, 'roe', 3)), roa: x.roa, lev: x.lev, pbr: x.pbr, pb: x.P.equity ? x.mcap / x.P.equity : null })));
ok('스크리닝 ROE 3Y평균·PBR 값이 분석 화면 계산과 같은 방식', vals.length === 1 && Math.abs(vals[0].roe3 - vals[0].hand) < 1e-12 && vals[0].roa > 0 && vals[0].roa < vals[0].roe3 && vals[0].lev > 5, JSON.stringify(vals[0]));
await pg.click('button:has-text("전부 해제")'); await pg.waitForTimeout(250);
ok('「전부 해제」하면 유형 선택도 풀림', (await pg.inputValue('#fscSec')) === '' && (await names()).length === 5);
const csv = await pg.evaluate(() => { let h = ''; const f = URL.createObjectURL; return typeof fscCsv === 'function'; }); ok('CSV 함수 존재', csv);

// 8) 백테스트 대상
await pg.click('#bn_admin'); await pg.waitForTimeout(150); await pg.evaluate(() => switchTab('backtest')); await pg.waitForTimeout(300);
await pg.evaluate(() => btLoadStocks()); await pg.waitForTimeout(1800);
const ids = async (mode) => pg.evaluate((md) => { document.getElementById('btUniv').value = md; const per = btBuildAnnual(['c_lnmcap'], { keepMissing: true }); const s = {}; per.forEach((p) => p.items.forEach((x) => { s[x.id] = 1; })); return Object.keys(s).sort(); }, mode);
ok('대상=전체: 5종목 모두', (await ids('all')).join() === 'B,F,G,H,I', (await ids('all')).join());
ok('대상=금융업 제외: 일반+지주(G,H)만', (await ids('exfin')).join() === 'G,H', (await ids('exfin')).join());
ok('대상=금융업·지주 제외: 일반(G)만', (await ids('exboth')).join() === 'G');
ok('대상=금융업만: B,F,I', (await ids('finonly')).join() === 'B,F,I');
const note = await pg.evaluate(() => { document.getElementById('btUniv').value = 'finonly'; return btUnivNote(); }); ok('대상 안내 문구에 사용 종목 수·이름 추정 수', note.includes('5개 중 3개 사용') && note.includes('이름 추정'), note);
await pg.evaluate(() => { document.getElementById('btUniv').value = 'all'; });

ok('페이지 오류·화면 그리기 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
