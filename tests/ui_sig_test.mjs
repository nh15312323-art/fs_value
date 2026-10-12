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
await openSec(pg, '체크리스트');
ok('체크리스트 12문항 + 보유 점검 5규칙', (await pg.$$('#sigChecklistBody input[type=checkbox]')).length === 12 && (await pg.innerText('#sigChecklistBody')).includes('즉시 익절한다'));
ok('초기 요약: 0/10, 매수 이유 미작성 경고', (await pg.innerText('#sigChkSum')).includes('0/12') && (await pg.innerText('#sigChkSum')).includes('미작성'));
await pg.click('#sigChecklistBody label:has-text("키워드 하나에만") input'); await pg.click('#sigChecklistBody label:has-text("재료·차트") input');
const ta = (await pg.$$('#sigChecklistBody textarea'));
ok('메모 입력칸 3개 (이유·반증·익절 계획)', ta.length === 3);
await ta[0].fill('영업이익 흑자 전환 + 거래량 증가'); await ta[1].fill('120일선 이탈 시'); await pg.waitForTimeout(150);
const sm = await pg.innerText('#sigChkSum');
ok('체크 2/12, 매수 이유 작성됨 표시', sm.includes('2/12') && sm.includes('작성됨'), sm);
ok('입력칸 글자 16px 이상, 가로 넘침 없음', await pg.evaluate(() => [].slice.call(document.querySelectorAll('#sigChecklistBody textarea, #sigG, #sigN')).every((e) => parseFloat(getComputedStyle(e).fontSize) >= 16) && document.documentElement.scrollWidth <= innerWidth + 1));
await openSec(pg, '재료 점검');
await pg.waitForTimeout(900);
const mt = await pg.innerText('#matBody');
ok('재료 점검: 공시 목록이 열면 로딩되어 표시(공급계약·실적·증자)', mt.includes('단일판매') && mt.includes('유상증자결정') && mt.includes('영업(잠정)실적'), mt.replace(/\n/g, ' ').slice(0, 160));
ok('재료 점검: 새로 나타남(수주)·사라짐(자금조달·주주환원) 표시', mt.includes('새로 나타남') && mt.includes('사라짐'));
ok('재료 점검: 정기보고서는 재료 목록에서 제외', !mt.includes('분기보고서'));
ok('재료 점검: 공시·뉴스 유무 큰 움직임 표와 뉴스 영역', mt.includes('큰 움직임') && mt.includes('뉴스 (네이버 검색 API · 제목 기준)'));
ok('재료 점검: 학술 근거와 한계 문구(미국 중심, 1년 최적 아님)', (await pg.innerText('#analysis')).includes('Chan 2003') && (await pg.innerText('#analysis')).includes('최적 관찰 기간'));
ok('재료 점검: 뉴스 월별 수·새로 등장(로봇)·사라짐(중국) 단어·반복도·최근 기사 링크', mt.includes('로봇') && mt.includes('중국') && mt.includes('같은 이야기의 반복도') && mt.includes('최근 기사') && (await pg.$$('#matBody a[href^="https://news.example/"]')).length >= 4, mt.slice(mt.indexOf('⑤'), mt.indexOf('⑤') + 200).replace(/\n/g, ' '));
ok('재료 점검: 모바일 가로 넘침 없음', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
await openSec(pg, '체크리스트');
ok('체크리스트 재료 항목이 공시 요약을 자동 참고로 표시', (await pg.innerText('#sigChecklistBody')).includes('최근 1년 재료성 공시'));
const sh = path.join(SHOT, 'sig_390.png');
const el = await pg.$('#sigChecklistBody'); await el.scrollIntoViewIfNeeded(); await pg.screenshot({ path: sh });
await pg.close();
// 다시 열어도 같은 종목의 체크·메모가 남아 있음
pg = await analyze(); await openSec(pg, '체크리스트');
const sm2 = await pg.innerText('#sigChkSum');
ok('재접속 후에도 체크·메모 유지(이 기기 저장)', sm2.includes('2/12') && sm2.includes('작성됨') && (await pg.$$eval('#sigChecklistBody textarea', (t) => t[0].value)).includes('흑자 전환'), sm2);
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
