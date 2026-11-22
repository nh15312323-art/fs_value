// 서버 쪽 확인: "경제 상황" 지표 확장 — (1) 월별 ECOS 조회(주기 M, YYYYMM)·이름 불일치 시 저장 거부 (2) 일별은 다음날부터, 주·월별은 마지막 저장일부터 다시 받기
// (3) /api/macro/series 기본은 기존 지표만, group=dash는 경제 상황 지표만. 실제 SQLite 사용, 외부 API는 가짜.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE macro_data (series TEXT, date TEXT, value REAL, PRIMARY KEY (series, date));');
const DB = { prepare: (sql) => { const st = db.prepare(sql); const mk = (b) => ({ first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }), run: async () => st.run(...b) }); const o = mk([]); o.bind = (...b) => mk(b); return o; } };
const ms0 = text.indexOf('const MACRO_SERIES = {'), ms1 = text.indexOf('const MACRO_LEGACY_KEYS', ms0); const ms1e = text.indexOf('\n', ms1) + 1;
const f0 = text.indexOf('async function fetchEcosSeries'), f1 = text.indexOf('// FRED(Federal', f0);
const a0 = text.indexOf('async function runMacroRefresh'), a1 = text.indexOf('// ====', a0);
const addDaysStr = (s, n) => { const d = new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8) + n)); return d.toISOString().slice(0, 10).replace(/-/g, ''); };
const multiRowInsert = async (D, head, cols, rows, f) => { for (const r of rows) db.prepare(head + ' VALUES (?,?,?)').run(...f(r)); };
(async () => {
  let lastUrl = '', body = '';
  globalThis.fetch = async (u) => { lastUrl = u; return { text: async () => body }; };
  const mod = new Function('addDaysStr', 'multiRowInsert', 'fetchFredSeries', 'fetchIndexHistory',
    text.slice(ms0, ms1e) + '\n' + text.slice(f0, f1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, fetchEcosSeries, MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS };');
  const fredCalls = [];
  const m = mod(addDaysStr, multiRowInsert, async (id, s, e) => { fredCalls.push(id + ':' + s); return [{ date: '20261014', value: 1 }, { date: '20261021', value: 2 }]; }, async () => []);
  const { runMacroRefresh, fetchEcosSeries, MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS } = m;
  // (1)
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '202609', DATA_VALUE: '2.50', ITEM_NAME1: '한국은행 기준금리' }, { TIME: '202610', DATA_VALUE: '2.50', ITEM_NAME1: '한국은행 기준금리' }] } });
  let pts = await fetchEcosSeries('722Y001', '0101000', '20260901', '20261031', 'KEY', 1000, '기준금리', 'M');
  check('월별 조회는 주기 M·YYYYMM으로 요청', /\/722Y001\/M\/202609\/202610\/0101000$/.test(lastUrl), lastUrl.replace('KEY', '***'));
  check('월 값은 그 달 1일로 저장 형식 변환', JSON.stringify(pts.map((p) => p.date)) === '["20260901","20261001"]');
  pts = await fetchEcosSeries('731Y001', '0000001', '20261001', '20261031', 'KEY', 1000, null, 'D');
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '20261001', DATA_VALUE: '1400', ITEM_NAME1: '원/달러' }] } });
  pts = await fetchEcosSeries('731Y001', '0000001', '20261001', '20261031', 'KEY', 1000, null, 'D');
  check('일별은 기존대로 D·YYYYMMDD', /\/731Y001\/D\/20261001\/20261031\/0000001$/.test(lastUrl) && pts[0].date === '20261001');
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '202610', DATA_VALUE: '9', ITEM_NAME1: '엉뚱한 계열' }] } });
  let err = null; try { await fetchEcosSeries('101Y004', 'BBHS00', '20261001', '20261031', 'KEY', 1000, 'M2', 'M'); } catch (e) { err = e; }
  check('항목 이름 불일치면 저장 거부(M2 코드 오입력 방지)', err && /불일치/.test(err.message), err && err.message.slice(0, 60));
  // (2)
  check('경제 상황 지표가 정의됨(미국 2·10년, TGA, 역레포, M2, 총자산)', ['us2y', 'us10y', 'ustga', 'usrrp', 'usm2', 'usbs'].every((k) => MACRO_SERIES[k] && MACRO_SERIES[k].dash));
  check('FRED 시리즈 ID가 공식 ID와 일치', MACRO_SERIES.us2y.seriesId === 'DGS2' && MACRO_SERIES.us10y.seriesId === 'DGS10' && MACRO_SERIES.usrrp.seriesId === 'RRPONTSYD' && MACRO_SERIES.ustga.seriesId === 'WDTGAL' && MACRO_SERIES.usbs.seriesId === 'WALCL' && MACRO_SERIES.usm2.seriesId === 'M2SL');
  check('기존 지표와 dash 지표가 겹치지 않고 합치면 전체', MACRO_DASH_KEYS.every((k) => !MACRO_LEGACY_KEYS.includes(k)) && MACRO_DASH_KEYS.length + MACRO_LEGACY_KEYS.length === Object.keys(MACRO_SERIES).length && MACRO_LEGACY_KEYS.includes('kospi'));
  db.exec("INSERT INTO macro_data VALUES ('ustga','20261014',100),('us2y','20261020',4.0),('usm2','20260901',22000)");
  const ecosCalls = [];
  body = JSON.stringify({ StatisticSearch: { row: [] } });
  const env = { DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' };
  fredCalls.length = 0;
  await runMacroRefresh(env, null, '20261030', '20250101');
  check('주별(TGA)·월별(M2)은 마지막 저장일부터 다시', fredCalls.includes('WDTGAL:20261014') && fredCalls.includes('M2SL:20260901'), fredCalls.join(','));
  check('일별(미국 2년)은 마지막 저장일 다음날부터', fredCalls.includes('DGS2:20261021'));
  check('처음 받는 지표는 기본 시작일부터', fredCalls.includes('DGS10:20250101'));
  const tga = db.prepare("SELECT date, value FROM macro_data WHERE series='ustga' ORDER BY date").all();
  check('다시 받은 주별 값은 덮어쓰기(중복행 없음)', tga.length === 2 && tga[0].value === 1 && tga[1].date === '20261021', JSON.stringify(tga));
  // (3) 라우트
  const i = text.indexOf('    if (pathname === "/api/macro/series") {'), j = text.indexOf('    return new Response("Not Found"', i);
  const h = new Function('env', 'searchParams', 'pathname', 'MACRO_SERIES', 'MACRO_DASH_KEYS', 'MACRO_LEGACY_KEYS', 'return (async () => {\n' + text.slice(i, j) + '\nreturn null; })();');
  db.exec("INSERT INTO macro_data VALUES ('kospi','20261020',2500)");
  const get = async (q) => (await h({ DB }, new URLSearchParams(q), '/api/macro/series', MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS)).json();
  const d0 = await get(''), d1 = await get('group=dash');
  check('기본 응답은 기존 지표만(상관 표 오염 방지)', d0.rows.length > 0 && d0.rows.every((r) => MACRO_LEGACY_KEYS.includes(r.series)) && d0.rows.some((r) => r.series === 'kospi'));
  check('group=dash는 경제 상황 지표만 + 주기 메타', d1.rows.length > 1 && d1.rows.every((r) => MACRO_DASH_KEYS.includes(r.series)) && d1.meta.ustga.freq === 'W' && d1.meta.usm2.freq === 'M');

  // (4) 새 지표 23개 최초 백필: 호출 1번의 subrequest(외부 조회+D1 실행)가 한도(50) 안이고, 여러 회차로 이어받아 전부 채워지는지
  {
    db.exec('DELETE FROM macro_data');
    let subs = 0; const countingDB = { prepare: (sql) => { const o = DB.prepare(sql); const wrap = (x) => ({ first: async () => { subs++; return x.first(); }, all: async () => { subs++; return x.all(); }, run: async () => { subs++; return x.run(); } }); const w = wrap(o); w.bind = (...b) => wrap(o.bind(...b)); return w; } };
    const mkPts = (start, end, monthly) => { const o = []; for (let t = Date.UTC(+start.slice(0, 4), +start.slice(4, 6) - 1, +start.slice(6, 8)); t <= Date.UTC(+end.slice(0, 4), +end.slice(4, 6) - 1, +end.slice(6, 8)); t += 86400000) { const d = new Date(t); const ymd = d.toISOString().slice(0, 10).replace(/-/g, ''); if (monthly ? ymd.slice(6) === '01' : d.getUTCDay() % 6) o.push({ date: ymd, value: 1 + o.length }); } return o; };
    let fetches = 0, maxSub = 0;
    const multiI = async (D, head, cols, rows, f) => { const per = 33; for (let i = 0; i < rows.length; i += per) { subs++; for (const r of rows.slice(i, i + per)) db.prepare(head + ' VALUES (?,?,?)').run(...f(r)); } };
    const m2 = new Function('addDaysStr', 'multiRowInsert', 'fetchFredSeries', 'fetchIndexHistory',
      text.slice(ms0, ms1e) + '\n' + text.slice(f0, f1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, MACRO_SERIES };')(addDaysStr, multiI,
      async (id, s, e) => { fetches++; subs++; return mkPts(s, e, MACRO_SERIES[Object.keys(MACRO_SERIES).find((k) => MACRO_SERIES[k].seriesId === id)].freq === 'M'); },
      async (nm, s, e) => { fetches += 3; subs += 3; return mkPts(s, e, false).map((p) => ({ basDt: p.date, clpr: p.value })); });
    globalThis.fetch = async () => { fetches++; subs++; return { text: async () => JSON.stringify({ StatisticSearch: { row: [{ TIME: '202610', DATA_VALUE: '1', ITEM_NAME1: '한국은행 기준금리 총지수 M2' }] } }) }; };
    const envC = { DB: countingDB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' };
    let rounds = 0, partial = true;
    while (partial && rounds < 80) { subs = 0; const sm = await m2.runMacroRefresh(envC, null, '20261030', '20171030'); rounds++; maxSub = Math.max(maxSub, subs); partial = Object.values(sm).some((x) => x && x.partial); }
    check('최초 백필이 80회차 안에 끝남(멈춤 없음)', !partial && rounds < 80, 'rounds=' + rounds);
    check('한 회차 subrequest가 무료 플랜 한도(50) 이하', maxSub <= 50, 'max=' + maxSub);
    const cnt = db.prepare("SELECT COUNT(DISTINCT series) AS n FROM macro_data").get().n;
    check('경제 상황 지표 포함 모든 FRED·지수 계열이 저장됨', cnt >= Object.values(m2.MACRO_SERIES).filter((d) => d.source !== 'ecos').length, 'series=' + cnt);
    subs = 0; await m2.runMacroRefresh(envC, null, '20261030', '20171030');
    check('모두 받은 뒤 다시 눌러도 subrequest 한도 이하', subs <= 50, 'subs=' + subs);
  }
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
