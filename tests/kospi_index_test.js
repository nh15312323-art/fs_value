// 서버 쪽 확인: (1) /api/price/series가 코스피를 종목 거래일에 맞춰 붙이는지(없는 날은 null, 실패해도 주가는 내려줌)
// (2) 거시 갱신(runMacroRefresh)이 코스피·S&P500을 macro_data에 올바른 계열명/날짜순으로 저장하는지. 실제 SQLite 사용, 외부 API는 가짜.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE corp_master (corp_name TEXT, stock_code TEXT); INSERT INTO corp_master VALUES (\'모의전자\',\'000001\');');
db.exec('CREATE TABLE market_index_daily (market_date TEXT, idx_name TEXT, close_price REAL, PRIMARY KEY (market_date, idx_name));');
db.exec('CREATE TABLE macro_data (series TEXT, date TEXT, value REAL, PRIMARY KEY (series, date));');
const DB = { prepare: (sql) => { const st = db.prepare(sql); const mk = (b) => ({ first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }), run: async () => st.run(...b) }); return { bind: (...b) => mk(b), ...mk([]) }; } };
// ---- (1) price/series
const i = text.indexOf('    if (pathname === "/api/price/series") {'), j = text.indexOf('    // 재료 점검용 뉴스', i);
if (i < 0 || j < 0) throw new Error('구간 없음');
const hist = ['20261026', '20261027', '20261028', '20261029'].map((d, k) => ({ date: d, close: 1000 + k, volume: 1, tradingValue: 1 }));
let idxMode = 'ok';
const mkHandler = () => new Function('env', 'searchParams', 'pathname', 'fetchKrxStockHistory', 'fetchIndexHistory', 'KOSPI_NAME', 'return (async () => {\n' + text.slice(i, j) + '\n return null; })()');
const h = mkHandler();
const run = async () => { const r = await h({ DB, DATA_GO_KR_KEY: 'k' }, new URLSearchParams({ corp_name: '모의전자' }), '/api/price/series', async () => hist,
  async () => { if (idxMode === 'throw') throw new Error('지수 API 오류'); return [{ basDt: '20261026', clpr: 2500 }, { basDt: '20261027', clpr: 2510 }, { basDt: '20261029', clpr: 2530 }, { basDt: '20261030', clpr: 9999 }]; }, '코스피'); return r.json(); };
(async () => {
  let a = await run();
  check('코스피가 종목 거래일에 맞춰 붙음(없는 날 null, 종목에 없는 날짜는 버림)', JSON.stringify(a.ki) === JSON.stringify([2500, 2510, null, 2530]), JSON.stringify(a.ki));
  check('종가·날짜는 그대로', a.c.length === 4 && a.d[0] === '20261026' && a.ki_src === '공공데이터포털 지수시세');
  idxMode = 'throw'; db.exec("INSERT INTO market_index_daily VALUES ('20261027','코스피',2511),('20261028','코스피',2520)");
  a = await run();
  check('지수 API 실패 시 저장된 코스피로 대체', JSON.stringify(a.ki) === JSON.stringify([null, 2511, 2520, null]) && /저장된/.test(a.ki_src), JSON.stringify(a.ki));
  db.exec('DELETE FROM market_index_daily');
  a = await run();
  check('둘 다 없으면 ki=null + 사유, 주가는 정상', a.ki === null && /지수 API 오류/.test(a.ki_err) && a.c.length === 4, a.ki_err);
  // ---- (2) runMacroRefresh
  const a0 = text.indexOf('async function runMacroRefresh'), a1 = text.indexOf('// ====', a0);
  const ms0 = text.indexOf('const MACRO_SERIES = {'), ms1 = text.indexOf('};', ms0) + 2;
  const addDaysStr = (s, n) => { const d = new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8) + n)); return d.toISOString().slice(0, 10).replace(/-/g, ''); };
  const multiRowInsert = async (D, head, cols, rows, f) => { for (const r of rows) { const v = f(r); db.prepare(head + ' VALUES (?,?,?)').run(...v); } };
  const calls = [];
  const factory = new Function('addDaysStr', 'multiRowInsert', 'fetchEcosSeries', 'fetchFredSeries', 'fetchIndexHistory',
    text.slice(ms0, ms1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, MACRO_SERIES };');
  const { runMacroRefresh, MACRO_SERIES } = factory(addDaysStr, multiRowInsert,
    async () => [{ date: '20261028', value: 1 }],
    async (id) => { calls.push('fred:' + id); return id === 'SP500' ? [{ date: '20261029', value: 6900.5 }, { date: '20261028', value: 6880 }] : [{ date: '20261028', value: 70 }]; },
    async (nm, s, e, k) => { calls.push('idx:' + nm); return [{ basDt: '20261029', clpr: 2530 }, { basDt: '20261028', clpr: 2520 }, { basDt: '20261027', clpr: 0 }, { basDt: 'bad', clpr: 1 }]; });
  check('MACRO_SERIES에 kospi·sp500 정의', MACRO_SERIES.kospi && MACRO_SERIES.kospi.source === 'krxidx' && MACRO_SERIES.sp500.seriesId === 'SP500');
  const sum = await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' }, null, '20261030', '20261001');
  const kp = db.prepare("SELECT date, value FROM macro_data WHERE series='kospi' ORDER BY date").all(), sp = db.prepare("SELECT date, value FROM macro_data WHERE series='sp500' ORDER BY date").all();
  check('코스피 저장(0값·잘못된 날짜 제외)', JSON.stringify(kp) === JSON.stringify([{ date: '20261028', value: 2520 }, { date: '20261029', value: 2530 }]), JSON.stringify(kp));
  check('S&P500 저장', sp.length === 2 && sp[1].value === 6900.5);
  check('요약에 두 지표 포함', sum.kospi && sum.kospi.points === 2 && sum.sp500 && sum.sp500.points === 2, JSON.stringify(sum.kospi));
  const s2 = await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f' }, null, '20261030', '20261001');
  check('DATA_GO_KR_KEY가 없으면 코스피만 오류로 표시하고 나머지는 계속', s2.kospi && /DATA_GO_KR_KEY/.test(s2.kospi.error || '') || (s2.kospi && s2.kospi.note), JSON.stringify(s2.kospi));
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
