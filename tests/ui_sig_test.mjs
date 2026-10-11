// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 ⑨ 시그널과 판단 점검(수익률 분해·기대수익률·변화 감지·체크리스트)을 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uisig_' + process.pid + '.mjs');
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

const SEC_A = { div: '26', name: '전자부품·컴퓨터·통신장비', n: 20, share: 0.5, sharePrev: 0.35, dPP: 15, relFlow: 0.43, ret20: 0.08, ret60: 0.2, rs20: 0.07, rs60: 0.17, small: false };
const SEC_B = { div: '10', name: '식료품', n: 19, share: 0.45, sharePrev: 0.6, dPP: -15, relFlow: -0.25, ret20: -0.02, ret60: 0.01, rs20: -0.03, rs60: -0.02, small: false };
const SEC_C = { div: '64', name: '금융', n: 2, share: 0.05, sharePrev: 0.05, dPP: 0, relFlow: 0.0, ret20: 0.01, ret60: 0.02, rs20: 0.0, rs60: -0.01, small: true };
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [{ n: '모의전자', c: '000001' }] }));
  if (p.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [{ corp_name: '모의전자', stock_code: '000001', n: rows.length, acc_mt: 12 }] }));
  if (p === '/api/db/usage') return r.end(JSON.stringify({ sizeBytes: 1e6, limitBytes: 5e8, limitMb: 500, warnPct: 90, pct: 0.2, warn: false }));
  if (p.startsWith('/api/sector/stock')) return r.end(JSON.stringify({ corp_name: '모의전자', induty_code: '26429', sector: '전자부품·컴퓨터·통신장비', div: '26', asof: dates[dates.length - 1], flowRank: 1, flowRankOf: 2, stats: SEC_A }));
  if (p.startsWith('/api/sector/flow')) return r.end(JSON.stringify({ asof: dates[dates.length - 1], recFrom: dates[dates.length - 20], recDays: 20, priDays: 60, hasPrior: true, base20: 0.01, base60: 0.03, stocksN: 41, universeN: 100, mappedN: 41, cached: false, at: Date.now(), sectors: [SEC_B, SEC_A, SEC_C] }));
  if (p.startsWith('/api/price/series')) return r.end(JSON.stringify({ corp_name: '모의전자', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: vol, tv }));
  if (p.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: '모의전자', close: close[close.length - 1], date: dates[dates.length - 1] }));
  if (p.startsWith('/api/financial-history-db')) return r.end(JSON.stringify({ corp_name: '모의전자', corp_code: 'X', acc_mt: 12, rows }));
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
const an = await pg.innerText('#analysis');
ok('종목분석에 ⑨ 시그널과 판단 점검 장 표시', an.includes('⑨ 시그널과 판단 점검'));
await openSec(pg, '수익률 분해');
const dec = await pg.evaluate(() => { const d = Array.prototype.slice.call(document.querySelectorAll('#analysis details.card')).filter((x) => x.querySelector('summary').textContent.indexOf('수익률 분해') >= 0)[0]; return d.innerText; });
ok('수익률 분해 표: 3년·5년 구간과 연도별 행, 항등식 설명', /연환산 3년/.test(dec) && /연환산 5년/.test(dec) && /2023→2024/.test(dec) && dec.includes('(1+EPS성장)×(1+PER변화)'), dec.replace(/\n/g, ' ').slice(0, 260));
ok('분해 표에 숫자(%)가 채워짐', (dec.match(/[+-]\d+\.\d%/g) || []).length >= 12, String((dec.match(/[+-]\d+\.\d%/g) || []).length));
await openSec(pg, '기대수익률');
const g0 = await pg.inputValue('#sigG');
ok('기대수익률 계산기: 기본 성장률 입력칸 채움', g0 !== '' && isFinite(Number(g0)), g0);
await pg.fill('#sigG', '12'); await pg.fill('#sigN', '4'); await pg.press('#sigN', 'Tab'); await pg.waitForTimeout(200);
const calc = await pg.innerText('#sigCalcBody');
ok('성장률·기간 변경 시 민감도 표가 갱신(EPS 성장 +12.0%, PER 4년간)', calc.includes('+12.0%') && calc.includes('PER 4년간') && calc.includes('예측이 아니라'), calc.replace(/\n/g, ' ').slice(0, 200));
await openSec(pg, '변화 감지');
const det = await pg.innerText('#sigDetectBody');
ok('변화 감지: 일별 주가 로딩 후 이동평균·거래·52주 시그널 표시', det.includes('이동평균 상태') && det.includes('52주 고점 대비') && /거래(대금|량)/.test(det), det.replace(/\n/g, ' ').slice(0, 220));
ok('시그널마다 근거 수준 배지(학술 검증/실무 관행 등)', (await pg.$$eval('#sigDetectBody span', (s) => s.map((x) => x.textContent).filter((t) => /학술 검증|실무 관행|가정·휴리스틱/.test(t)).length)) >= 3);
await openSec(pg, '변화 감지');
const det2 = await pg.$eval('#sigDetectBody', (e) => e.textContent);
ok('변화 감지: 소속 업종 시황(상대 유입 +43%·상대강도) 표시', det2.includes('소속 업종 전자부품·컴퓨터·통신장비') && det2.includes('+43%') && det2.includes('상대 유입 순위 1/2'), det2.replace(/\n/g, ' ').slice(0, 300));
await openSec(pg, '체크리스트');
ok('체크리스트 9번(시황)에 업종 흐름이 자동 참고로 표시', (await pg.innerText('#sigChecklistBody')).includes('자동 참고: 최근 6개월') && (await pg.innerText('#sigChecklistBody')).includes('소속 업종 전자부품'));
ok('체크리스트 10문항 + 보유 점검 5규칙', (await pg.$$('#sigChecklistBody input[type=checkbox]')).length === 10 && (await pg.innerText('#sigChecklistBody')).includes('즉시 익절한다'));
ok('초기 요약: 0/10, 매수 이유 미작성 경고', (await pg.innerText('#sigChkSum')).includes('0/10') && (await pg.innerText('#sigChkSum')).includes('미작성'));
await pg.click('#sigChecklistBody label:has-text("키워드 하나에만") input'); await pg.click('#sigChecklistBody label:has-text("재료·차트") input');
const ta = (await pg.$$('#sigChecklistBody textarea'));
ok('메모 입력칸 3개 (이유·반증·익절 계획)', ta.length === 3);
await ta[0].fill('영업이익 흑자 전환 + 거래량 증가'); await ta[1].fill('120일선 이탈 시'); await pg.waitForTimeout(150);
const sm = await pg.innerText('#sigChkSum');
ok('체크 2/10, 매수 이유 작성됨 표시', sm.includes('2/10') && sm.includes('작성됨'), sm);
ok('입력칸 글자 16px 이상, 가로 넘침 없음', await pg.evaluate(() => [].slice.call(document.querySelectorAll('#sigChecklistBody textarea, #sigG, #sigN')).every((e) => parseFloat(getComputedStyle(e).fontSize) >= 16) && document.documentElement.scrollWidth <= innerWidth + 1));
const sh = path.join(SHOT, 'sig_390.png');
const el = await pg.$('#sigChecklistBody'); await el.scrollIntoViewIfNeeded(); await pg.screenshot({ path: sh });
await pg.close();
// 다시 열어도 같은 종목의 체크·메모가 남아 있음
pg = await analyze(); await openSec(pg, '체크리스트');
const sm2 = await pg.innerText('#sigChkSum');
ok('재접속 후에도 체크·메모 유지(이 기기 저장)', sm2.includes('2/10') && sm2.includes('작성됨') && (await pg.$$eval('#sigChecklistBody textarea', (t) => t[0].value)).includes('흑자 전환'), sm2);

// ---- 매크로 탭: 섹터 유동성
const pm = await ctx.newPage(); pm.on('pageerror', (e) => errs.push(String(e)));
await pm.goto(base); await pm.waitForTimeout(500);
await pm.click('#bn_more'); await pm.click('#mn_macro'); await pm.waitForTimeout(800);
ok('매크로 탭을 열면 섹터 표가 자동으로 표시됨', await pm.isVisible('#secWrap') && (await pm.$$('#secWrap tbody tr')).length === 3);
const names = await pm.$$eval('#secWrap tbody tr', (rs) => rs.map((r) => r.children[0].textContent));
ok('기본 정렬: 상대 유입 큰 순, 소표본(⚠)은 맨 아래', names[0].includes('전자부품') && names[2].includes('금융') && names[2].includes('⚠'), names.join(' | '));
await pm.click('#secS_share'); await pm.waitForTimeout(100);
ok('정렬 버튼: 비중순으로 바뀜(식료품 45% < 전자 50% → 전자 먼저, 변화 없음 확인용 활성 상태)', (await pm.getAttribute('#secS_share', 'class')) === 'toggle-active');
await pm.click('#secS_rs20'); await pm.waitForTimeout(100);
const n2 = await pm.$$eval('#secWrap tbody tr', (rs) => rs.map((r) => r.children[0].textContent));
ok('20일 상대강도순: 전자(+7%) → 식료품(−3%) → 금융(소표본)', n2[0].includes('전자부품') && n2[1].includes('식료품') && n2[2].includes('금융'), n2.join(' | '));
const st2 = await pm.innerText('#secStatus');
ok('상태: 기준일·분류 완료 41/100(41%)·분류 미완료 경고', st2.includes('41/100') && st2.includes('41%') && st2.includes('업종 분류가 아직 끝나지 않았습니다'), st2.replace(/\n/g, ' ').slice(0, 200));
ok('읽는 법·한계 설명, 매수 신호 아님 안내', (await pm.innerText('#secNote')).includes('한계') && (await pm.innerText('#tabMacro')).includes('매수 신호가 아닙니다'));
const mm = await pm.evaluate(() => ({ over: document.documentElement.scrollWidth - innerWidth, btn: [].slice.call(document.querySelectorAll('#tabMacro button')).filter((e) => e.getBoundingClientRect().height > 0 && e.getBoundingClientRect().height < 39.5).map((e) => e.textContent.trim()) }));
ok('매크로 탭: 가로 넘침 없음·버튼 40px 이상', mm.over <= 1 && mm.btn.length === 0, JSON.stringify(mm));
await pm.screenshot({ path: path.join(SHOT, 'sec_390.png') });
await pm.close();
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
