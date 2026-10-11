// 사용법: node tests/core_test.js            (src/index.js의 실제 함수 원문을 잘라와 실행; 소스는 수정하지 않음)
// 검증 대상: ① 누적→단독 분기→TTM 계산 ② 공시 다음 거래일 가격 선택 ③ 연간 백테스트의 매매가·제외 건수
const fs = require('fs'), path = require('path');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (start, end) => {
  const a = text.indexOf(start); const b = text.indexOf(end, a);
  if (a < 0 || b < 0) throw new Error('소스에서 구간을 찾지 못했습니다: ' + start + ' ~ ' + end);
  return text.slice(a, b);
};
const unescapeTpl = (t) => t.replace(/\\`/g, '`').replace(/\\\$\{/g, '${');

const keys = between('const FLOW_KEYS = [', '\nconst ACCOUNT_ITEMS').replace(/^const /gm, 'var ');
const server = between('const REPRT_CODE_BY_Q', 'async function fetchPeriodRow');
const nextDay = between('function firstCloseAfter', 'function closeAsOf');
let client = unescapeTpl(between('function toQuarterlyRows(rows) {', 'function toAnnualRows'));
const annualSrc = unescapeTpl(between('function btBuildAnnual(keys, o) {', '// 분기 모멘텀:'));
const lineOf = (prefix) => { const l = text.split('\n').find((x) => x.startsWith(prefix)); if (!l) throw new Error('소스에서 줄을 찾지 못했습니다: ' + prefix); return l; };
const fcfSrc = between('function anFcfType(c) {', '// ---------- 전체 컨텍스트');
const screenExtra = between('const SCREEN_MOM_DAYS', 'const SCREEN_LIST_KEYS');
const fiscalClient = unescapeTpl(between('function btSplitFiscal(comps) {', 'async function checkFiscalMonths'));
const priceFns = between('function smaSeries(arr, n) {', 'let PRICE = {');
const code = [
  screenExtra, priceFns,
  keys, server, nextDay, fiscalClient, // server 구간에 결산월 함수가 이미 포함됨
  [lineOf('const AN_TH ='), lineOf('const anFin ='), lineOf('const anDiv ='), lineOf('const anPct ='), lineOf('const anMean ='), lineOf('const anWin ='), 'const anFmt = (k, v) => String(v);'].join('\n'),
  fcfSrc,
  'const FLOW_KEYS_ = FLOW_KEYS; const CF_CUM_KEYS = CF_CUMULATIVE_KEYS; const isUsable = (r) => !r.error && r.fs_div != null;',
  client.replace(/\bFLOW_KEYS\b/g, 'FLOW_KEYS_'),
  'let BT = { stocks: [] };',
  'const anBuildSeries = (rows) => { const ys = rows.filter((r) => r.reprt_code === "11011").map((r) => Number(r.bsns_year)); return ys.length ? [{ year: Math.max(...ys) }] : []; };',
  'const BT_FACTORS = { f: { fn: () => 1 } }; const btRankAvg = (a) => a.map((_, i) => i / a.length);',
  annualSrc,
  'return { smaSeries, drawdownSeries, priceJumps, priceStats, SCREEN_EXTRA_SQL, SCREEN_JUMP_LIMIT, screenExtraMetric, getTTMFlow, getIsolatedQ4, toQuarterlyRows, firstCloseAfter, btBuildAnnual, anFcfType, parseAccMt, fiscalSkipDates, btSplitFiscal, fiscalBanner, setStocks: (s) => { BT.stocks = s; }, getBT: () => BT };',
].join('\n');
const T = new Function(code)();

let fail = 0;
const check = (name, pass, detail) => { if (!pass) fail++; console.log((pass ? 'PASS' : 'FAIL') + '  ' + name + (detail !== undefined ? '  ' + detail : '')); };
const near = (a, b) => a != null && Math.abs(a - b) < 1e-9;

function mkDb(rows) {
  return { prepare: () => ({ bind: (...b) => ({ first: async () => {
    const [corp, yr, code] = b; const r = rows.find((x) => x.corp_code === corp && x.bsns_year === yr && x.reprt_code === code);
    return r ? { ...r } : null; } }) }) };
}
const NI = { 2024: [10, 20, 30, 40], 2025: [15, 25, 35, 45] };          // 손익: 분기 단독값
const OCFq = { 2024: [5, 7, 9, 19], 2025: [6, 9, 12, 50] };             // 현금흐름: 분기 단독 정답
const cum = (a) => a.reduce((s, v, i) => (s.push((s[i - 1] || 0) + v), s), []);
const rows = [];
for (const y of [2024, 2025]) {
  const c = cum(OCFq[y]);
  [['11013', 0], ['11012', 1], ['11014', 2], ['11011', 3]].forEach(([code, i]) => {
    rows.push({ corp_code: 'X', bsns_year: String(y), reprt_code: code, fs_div: 'CFS', net_income: code === '11011' ? NI[y].reduce((a, b) => a + b, 0) : NI[y][i], ocf: c[i] });
  });
}

(async () => {
  const db = mkDb(rows);
  // ① TTM
  check('TTM 순이익 @2025Q3 = 40+15+25+35', near((await T.getTTMFlow(db, 'X', 2025, 3, ['net_income'], null)).net_income, 115));
  check('TTM OCF @2025Q3 = 19+6+9+12', near((await T.getTTMFlow(db, 'X', 2025, 3, ['ocf'], null)).ocf, 46));
  check('TTM OCF @2025Q1 = 7+9+19+6', near((await T.getTTMFlow(db, 'X', 2025, 1, ['ocf'], null)).ocf, 41));
  check('TTM 순이익 @2025Q4(연간) = 120', near((await T.getTTMFlow(db, 'X', 2025, 4, ['net_income'], null)).net_income, 120));
  check('4분기 단독 순이익 2024 = 40', near((await T.getIsolatedQ4(db, 'X', 2024, ['net_income'], null)).net_income, 40));
  const q = T.toQuarterlyRows(rows.map((r) => ({ ...r }))); const g = (l, k) => (q.find((r) => r.period_label === l) || {})[k];
  check('화면 2025 3분기 OCF 단독 = 12', near(g('2025 3분기', 'ocf'), 12));
  check('화면 2025 4분기 OCF 단독 = 50', near(g('2025 4분기', 'ocf'), 50));
  const mixedIS = rows.map((r) => (r.bsns_year === '2025' && r.reprt_code === '11012') ? { ...r, fs_div: 'OFS' } : { ...r });
  check('연결/개별 혼합 → 손익 TTM null', (await T.getTTMFlow(mkDb(mixedIS), 'X', 2025, 3, ['net_income'], null)) === null);
  const mixedCF = rows.map((r) => (r.bsns_year === '2025' && r.reprt_code === '11014') ? { ...r, fs_div: 'OFS' } : { ...r });
  check('연결/개별 혼합 → 현금흐름 TTM null', (await T.getTTMFlow(mkDb(mixedCF), 'X', 2025, 3, ['ocf'], null)).ocf === null);
  check('분기 누락 → TTM null', (await T.getTTMFlow(mkDb(rows.filter((r) => !(r.bsns_year === '2025' && r.reprt_code === '11013'))), 'X', 2025, 3, ['net_income'], null)) === null);

  // ② 공시 다음 거래일: 당일은 포함하지 않는다
  const px = [{ date: '20250328', close: 100 }, { date: '20250331', close: 110 }, { date: '20250401', close: 120 }, { date: '20250402', close: 130 }];
  const n1 = T.firstCloseAfter(px, '20250331');
  check('다음 거래일 = 공시 당일 제외', n1 && n1.date === '20250401' && n1.close === 120, JSON.stringify(n1));
  const n2 = T.firstCloseAfter(px, '20250329'); // 토요일 공시 → 월요일
  check('휴일 공시 → 이후 첫 거래일', n2 && n2.date === '20250331', JSON.stringify(n2));
  check('이후 시세 없으면 null', T.firstCloseAfter(px, '20250402') === null);

  // ③ 연간 백테스트: 매매가는 price_next_day, 공시 당일가(price_at_filing)는 쓰지 않는다
  const ann = (y, fd, atF, nxt) => ({ corp_code: 'A', bsns_year: String(y), reprt_code: '11011', fs_div: 'CFS', filing_date: fd, price_at_filing: atF, price_next_day: nxt });
  const mk = (code, nxt1) => ({ code, rows: [ann(2023, '20240320', 1000, 1100), ann(2024, '20250320', 1500, nxt1)] });
  T.setStocks([mk('A', 1210), mk('B', null)]);
  const periods = T.btBuildAnnual(['f'], {});
  const item = periods.length ? periods[0].items.find((x) => x.id === 'A') : null;
  check('수익률 = next_day 기준 1210/1100-1 (공시일 가격 1500/1000-1 아님)', item && near(item.f, 1210 / 1100 - 1), item ? 'f=' + item.f : 'no item');
  check('다음 거래일 가격 없는 종목은 제외·집계', !periods[0].items.some((x) => x.id === 'B') && T.getBT().skipNoNext === 1, 'skip=' + T.getBT().skipNoNext);
  // ④ FCF 유형 분류
  const pt = (year, ocf, capex, revenue, shares) => ({ year, ocf, capex, fcf: (ocf != null && capex != null) ? ocf - capex : null, revenue, shares: shares || 1000,
    fcfm: (ocf != null && capex != null) ? (ocf - capex) / revenue : null, capexRev: capex != null ? capex / revenue : null, cfoni: 1.0 });
  const mkc = (list, o) => Object.assign({ list, growthRev: 0.08, fcfDown: false, fcfm3: 0.10, fcfm5: 0.09 }, o || {});
  const names = (r) => (r.head.text || '');
  const comp = T.anFcfType(mkc([pt(2021, 100, 40, 1000), pt(2022, 110, 40, 1100), pt(2023, 125, 45, 1200), pt(2024, 140, 50, 1300), pt(2025, 160, 55, 1400)]));
  check('꾸준한 FCF 흑자·매출 성장 → Compounder', names(comp).includes('FCF Compounder') && !names(comp).includes('Emerging'), comp.head.text);
  const emer = T.anFcfType(mkc([pt(2021, 20, 60, 500), pt(2022, 30, 70, 600), pt(2023, 40, 60, 700), pt(2024, 60, 55, 800), pt(2025, 90, 50, 900)], { fcfm3: 0.01, fcfm5: -0.03, fcfDown: false }));
  check('적자 후 흑자 전환·CFO 개선 → Emerging (FCF Compounder 아님)', names(emer).includes('Emerging') && !names(emer).includes('FCF Compounder'), emer.head.text);
  const reinv = T.anFcfType(mkc([pt(2021, 100, 85, 1000), pt(2022, 120, 105, 1200), pt(2023, 150, 135, 1500), pt(2024, 190, 175, 1900), pt(2025, 240, 225, 2400)], { growthRev: 0.25, fcfm3: 0.01, fcfm5: 0.012, fcfDown: true }));
  check('CFO 증가·CAPEX 과다·FCF 낮음 → Growth Reinvestment', names(reinv).includes('Growth Reinvestment'), reinv.head.text);
  check('CFO 마진이 매년 같으면 악화로 보지 않음(소수점 오차 방어)', reinv.types.find((t) => t.name === 'Growth Reinvestment').conds[5][1] === true);
  const trap = T.anFcfType(mkc([pt(2021, 50, 20, 1000, 1000), pt(2022, -10, 25, 1200, 1050), pt(2023, 5, 30, 1400, 1150), pt(2024, -20, 35, 1600, 1250), pt(2025, -30, 40, 1800, 1400)], { growthRev: 0.15, fcfm3: -0.04, fcfm5: -0.01, fcfDown: true }));
  check('매출 성장·CFO 적자·희석 → Growth Trap', names(trap).includes('Growth Trap'), trap.head.text);
  check('Trap의 반대 근거에 희석 경고 포함', trap.counters.some((t) => t.includes('희석')), JSON.stringify(trap.counters));
  const thin = T.anFcfType(mkc([pt(2024, 100, 40, 1000), pt(2025, 110, 40, 1100)]));
  check('FCF 연도 부족(2개) → 판정 보류, 유형 없음', thin.types.length === 0 && thin.head.text.includes('판정 보류'), thin.head.text);
  const nocx = T.anFcfType(mkc([pt(2021, 100, null, 1000), pt(2022, 110, null, 1100), pt(2023, 125, null, 1200), pt(2024, 140, null, 1300), pt(2025, 160, null, 1400)]));
  check('CAPEX 미확인(FCF 없음)이면 흑자로 가정하지 않고 판정 보류', nocx.types.length === 0 && nocx.nF === 0, nocx.head.text);
  // ⑤ 12-1 모멘텀·52주 고가 근접도 (SQL은 node:sqlite로 실제 실행)
  const { DatabaseSync } = require('node:sqlite');
  const sdb = new DatabaseSync(':memory:');
  sdb.exec('CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, PRIMARY KEY (market_date, stock_code))');
  const days = []; for (let i = 0; i < 260; i++) days.push(String(20250101 + i)); // 임의의 오름차순 날짜 260개(달력 날짜 형식은 무관)
  const insP = sdb.prepare('INSERT INTO market_raw_daily VALUES (?,?,?)');
  days.forEach((d, i) => {
    insP.run(d, 'A0', 100 + i);                         // 꾸준한 상승: 마지막이 신고가
    insP.run(d, 'B0', i < 130 ? 200 : 100);              // 하루에 -50% (분할 의심)
    if (i >= 100) insP.run(d, 'C0', 100);               // 이력 짧음(160일)
    insP.run(d, 'D0', i === 200 ? 300 : 200);           // 고점 후 되돌림(+50% 후 -33%): 둘 다 30% 초과 → 불연속
    insP.run(d, 'E0', i <= 150 ? 100 + i : 250 - (i - 150) * 0.2); // 150일째 정점 후 완만한 하락(일변동 <30%)
  });
  const dStart = days[0], dEnd = days[days.length - 1], d21 = days[days.length - 1 - 21];
  const exm = {}; sdb.prepare(T.SCREEN_EXTRA_SQL.replace(/\?(\d)/g, '?$1')).all(dStart, dEnd, d21, T.SCREEN_JUMP_LIMIT).forEach((r) => { exm[r.sc] = r; });
  const last = (code) => sdb.prepare('SELECT close_price FROM market_raw_daily WHERE stock_code=? AND market_date=?').get(code, dEnd).close_price;
  const mA = T.screenExtraMetric(exm.A0, last('A0'));
  check('A: 12-1 = c21/c252-1, 신고가면 근접도 1.00', mA.extra === 'ok' && near(mA.m121, (100 + 259 - 21) / 100 - 1) && near(mA.hi52, 1), JSON.stringify(mA));
  check('B: 하루 -50% → 가격 불연속, 값 null(분할 가능성)', T.screenExtraMetric(exm.B0, last('B0')).extra === 'jump' && T.screenExtraMetric(exm.B0, last('B0')).m121 === null);
  check('C: 이력 160일 → 자료 부족, 값 null(채우지 않음)', T.screenExtraMetric(exm.C0, last('C0')).extra === 'short' && T.screenExtraMetric(exm.C0, last('C0')).hi52 === null);
  check('D: 일시 급등락(±30% 초과) → 불연속', T.screenExtraMetric(exm.D0, last('D0')).extra === 'jump');
  const mE = T.screenExtraMetric(exm.E0, last('E0'));
  check('E: 완만한 하락 → 고가 대비 < 1, 불연속 아님', mE.extra === 'ok' && mE.hi52 < 1 && near(mE.hi52, last('E0') / 250), JSON.stringify(mE));
  check('종목 자체가 없으면 nodata', T.screenExtraMetric(undefined, 100).extra === 'nodata');
  // ⑤ 결산월
  check('결산월 파싱: "12" / "03" / 3', T.parseAccMt({ status: '000', acc_mt: '12' }) === 12 && T.parseAccMt({ status: '000', acc_mt: '03' }) === 3 && T.parseAccMt({ status: '000', acc_mt: 3 }) === 3);
  check('결산월 파싱: 오류 응답·범위 밖·누락은 null', T.parseAccMt({ status: '013' }) === null && T.parseAccMt({ status: '000', acc_mt: '13' }) === null && T.parseAccMt({ status: '000' }) === null && T.parseAccMt(null) === null);
  check('날짜 기반 값 생략 조건: 확인된 비12월만', T.fiscalSkipDates(3) === true && T.fiscalSkipDates(12) === false && T.fiscalSkipDates(null) === false);
  const sp = T.btSplitFiscal([{ corp_name: 'A', acc_mt: 12 }, { corp_name: 'B', acc_mt: 3 }, { corp_name: 'C', acc_mt: null }, { corp_name: 'D', acc_mt: 9 }]);
  check('백테스트 분리: 12월·미확인은 사용, 확인된 비12월은 제외', sp.use.map((c) => c.corp_name).join() === 'A,C' && sp.excluded.map((c) => c.corp_name).join() === 'B,D' && sp.unknownN === 1, JSON.stringify({ use: sp.use.length, ex: sp.excluded.length, unk: sp.unknownN }));
  check('경고 배너: 비12월에만 표시', T.fiscalBanner(3).includes('결산월이 3월') && T.fiscalBanner(12) === '' && T.fiscalBanner(null) === '');

  // fetchPeriodRow 통합 검사: 모의 DART·시세 응답으로 결산월별 동작 확인
  const os = require('os'), { pathToFileURL } = require('url');
  const tmp = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'fsv-')), 'idx.mjs');
  fs.writeFileSync(tmp, text + '\nexport { fetchPeriodRow };\n');
  const { fetchPeriodRow } = await import(pathToFileURL(tmp).href);
  const dates = []; for (let d = new Date(Date.UTC(2024, 11, 10)); d <= new Date(Date.UTC(2025, 3, 20)); d.setUTCDate(d.getUTCDate() + 1)) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); }
  const naverText = "[['날짜','시가','고가','저가','종가','거래량']," + dates.map((x, i) => '["' + x + '","1","1","1","' + (100 + i) + '","10"]').join(',') + ']';
  let calls = {};
  const mockFetch = (companyReply) => async (url) => {
    const u = new URL(url); const ep = u.searchParams.get('endpoint'); const key = u.searchParams.get('source') === 'naver_price' ? 'naver' : ep;
    calls[key] = (calls[key] || 0) + 1;
    const json = (o) => ({ json: async () => o, text: async () => JSON.stringify(o) });
    if (key === 'naver') return { text: async () => naverText, json: async () => ({}) };
    if (ep === 'fnlttSinglAcntAll') return json({ status: '000', list: [{ rcept_no: '20250320000123', sj_div: 'IS', account_id: 'ifrs-full_Revenue', account_nm: '매출액', account_detail: '-', thstrm_amount: '1,000' }] });
    if (ep === 'company') return json(companyReply);
    return json({ status: '013' });
  };
  const mkFiscalDb = (initial) => { const st = { fiscal: initial == null ? null : initial, inserts: 0 };
    return { st, prepare: (sql) => ({ run: async () => ({}), bind: (...b) => ({
      first: async () => (/FROM corp_fiscal/.test(sql) ? (st.fiscal == null ? null : { acc_mt: st.fiscal }) : null),
      run: async () => { if (/INSERT OR REPLACE INTO corp_fiscal/.test(sql)) { st.fiscal = b[1]; st.inserts++; } return {}; },
      all: async () => ({ results: [] }) }) }) }; };
  const PERIOD = { year: 2024, code: '11011', label: '사업보고서', order: 4 };
  const realFetch = globalThis.fetch;
  try {
    calls = {}; globalThis.fetch = mockFetch({ status: '000', acc_mt: '03' }); let db = mkFiscalDb(null);
    let r = await fetchPeriodRow('C_MAR', '000001', PERIOD, 'https://proxy.example/exec', db);
    check('3월 결산: 결산월을 조회·저장하고 날짜 기반 값은 만들지 않음', db.st.fiscal === 3 && r.price_at_filing == null && r.price_at_period_end == null && r.price_next_day == null && r.filing_date == null && !calls.naver, JSON.stringify({ fiscal: db.st.fiscal, pf: r.price_at_filing, naver: calls.naver || 0 }));
    check('3월 결산이어도 재무 값 자체는 정상 저장 대상(매출 1000)', r.revenue === 1000 && r.fs_div === 'CFS', 'revenue=' + r.revenue);
    calls = {}; globalThis.fetch = mockFetch({ status: '000', acc_mt: '12' }); db = mkFiscalDb(null);
    r = await fetchPeriodRow('C_DEC', '000002', PERIOD, 'https://proxy.example/exec', db);
    check('12월 결산: 기존처럼 공시일·다음 거래일 가격 계산', db.st.fiscal === 12 && r.filing_date === '20250320' && r.price_at_filing > 0 && r.price_next_day > r.price_at_filing && r.price_at_period_end > 0, JSON.stringify({ f: r.filing_date, p: r.price_at_filing, n: r.price_next_day }));
    calls = {}; globalThis.fetch = mockFetch({ status: '900', message: '미지원' }); db = mkFiscalDb(null);
    r = await fetchPeriodRow('C_FAIL', '000003', PERIOD, 'https://proxy.example/exec', db);
    await fetchPeriodRow('C_FAIL', '000003', { ...PERIOD, year: 2023 }, 'https://proxy.example/exec', db);
    check('조회 실패: 12월로 가정해 기존대로 계산하고 저장은 안 함, 같은 회사를 반복 조회하지 않음', db.st.fiscal == null && r.price_at_filing > 0 && calls.company === 1, JSON.stringify({ fiscal: db.st.fiscal, company: calls.company }));
    calls = {}; globalThis.fetch = mockFetch({ status: '000', acc_mt: '12' }); db = mkFiscalDb(9);
    r = await fetchPeriodRow('C_KNOWN', '000004', PERIOD, 'https://proxy.example/exec', db);
    check('이미 9월 결산으로 저장됨: 회사개요를 다시 조회하지 않고 날짜 기반 값 생략', !calls.company && r.price_at_filing == null && !calls.naver, JSON.stringify(calls));
  } finally { globalThis.fetch = realFetch; }
  // ⑥ 주가 차트 계산(이동평균·낙폭·불연속·52주 통계)
  const sm = T.smaSeries([1, 2, 3, 4, 5, 6], 3);
  check('이동평균: 처음 n-1개는 null, 이후 평균', sm[0] === null && sm[1] === null && near(sm[2], 2) && near(sm[5], 5), JSON.stringify(sm));
  const ddn = T.drawdownSeries([100, 120, 90, 130, 65]);
  check('낙폭: 고점 대비(120→90 = -25%, 130→65 = -50%)', near(ddn[0], 0) && near(ddn[2], -0.25) && near(ddn[3], 0) && near(ddn[4], -0.5), JSON.stringify(ddn));
  check('불연속: -50%·+40%는 감지, 29%는 아님', JSON.stringify(T.priceJumps([100, 50, 70, 90, 116.1])) === JSON.stringify([1, 2]), JSON.stringify(T.priceJumps([100, 50, 70, 90, 116.1])));
  const series = Array.from({ length: 300 }, (_, i) => 100 + i);   // 단조 상승 300일
  const ps = T.priceStats(series);
  check('52주 통계: 신고가 → 고가 대비 0%, 저가는 최근 252일 최소값', near(ps.fromHi, 0) && ps.lo === 100 + 300 - 252 && ps.nWin === 252 && ps.vsM200 > 0 && near(ps.maxDD, 0), JSON.stringify(ps));
  check('데이터 200일 미만이면 200일선 대비 = null(만들지 않음)', T.priceStats(series.slice(0, 120)).vsM200 === null);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과');
  process.exit(fail ? 1 : 0);
})();
