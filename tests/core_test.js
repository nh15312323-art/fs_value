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
const code = [
  keys, server, nextDay,
  'const FLOW_KEYS_ = FLOW_KEYS; const CF_CUM_KEYS = CF_CUMULATIVE_KEYS; const isUsable = (r) => !r.error && r.fs_div != null;',
  client.replace(/\bFLOW_KEYS\b/g, 'FLOW_KEYS_'),
  'let BT = { stocks: [] }; const anFin = (v) => typeof v === "number" && isFinite(v);',
  'const anBuildSeries = (rows) => { const ys = rows.filter((r) => r.reprt_code === "11011").map((r) => Number(r.bsns_year)); return ys.length ? [{ year: Math.max(...ys) }] : []; };',
  'const BT_FACTORS = { f: { fn: () => 1 } }; const btRankAvg = (a) => a.map((_, i) => i / a.length);',
  annualSrc,
  'return { getTTMFlow, getIsolatedQ4, toQuarterlyRows, firstCloseAfter, btBuildAnnual, setStocks: (s) => { BT.stocks = s; }, getBT: () => BT };',
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
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과');
  process.exit(fail ? 1 : 0);
})();
