// 일일 갱신 보완 테스트: node tests/catchup_test.js (Node 22+ node:sqlite)
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('// ===== 일일 갱신 보완'), j = text.indexOf('// ===== /일일 갱신 보완 =====');
if (i < 0 || j < 0) throw new Error('구간 없음');
const calls = { uni: 0, kospi: 0, range: [] };
const stubs = "const KOSPI_NAME = '코스피'; const __s = globalThis.__stub;" +
  "async function runUniverseRefresh(env, f) { __s.calls.uni++; if (__s.uniErr) throw new Error(__s.uniErr); return __s.uniRes; }" +
  "async function refreshKospi() { __s.calls.kospi++; if (__s.kErr) throw new Error(__s.kErr); }" +
  "async function startRangeJob(env, a, b) { __s.calls.range.push([a, b]); return __s.rangeRes || { ok: true }; }";
globalThis.__stub = { calls, uniRes: { bas_dt: '20251012', job_started: true }, uniErr: null, kErr: null, rangeRes: null };
const M = new Function(stubs + text.slice(i, j) + '\nreturn { priceGapDates, findPriceGaps, runDailyCatchUp };')();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
function mkEnv(calDates, rawDates, jobs) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE market_index_daily (market_date TEXT, idx_name TEXT, close_price REAL); CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, PRIMARY KEY (market_date, stock_code)); CREATE TABLE market_job (k TEXT PRIMARY KEY, v TEXT);');
  calDates.forEach((d) => db.prepare('INSERT INTO market_index_daily VALUES (?,?,?)').run(d, '코스피', 2500));
  rawDates.forEach((d) => { db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?)').run(d, '000010', 1); db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?)').run(d, '000020', 1); });
  Object.entries(jobs || {}).forEach(([k, v]) => db.prepare('INSERT INTO market_job VALUES (?,?)').run(k, JSON.stringify(v)));
  const wrap = (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ _st: st, _b: b, all: async () => ({ results: st.all(...b) }), first: async () => st.get(...b) || null, run: async () => st.run(...b) }), all: async () => ({ results: st.all() }), first: async () => st.get() || null, run: async () => st.run() }; };
  return { db, DB: { prepare: wrap, batch: async (arr) => arr.map((a) => ({ results: a._st.all(...a._b) })) } };
}
(async () => {
  const cal = ['20251013', '20251012', '20251008', '20251007', '20251006', '20251002'];
  check('빠진 날 = 저장된 최신일보다 오래됐는데 시세 없는 날(오름차순)', JSON.stringify(M.priceGapDates(cal, new Set(['20251012', '20251006', '20251002']))) === JSON.stringify(['20251007', '20251008']));
  check('저장된 최신일(10/06) 이후의 날(10/07·08·12, 아직 못 받은 날)은 구멍으로 보지 않음', M.priceGapDates(cal, new Set(['20251006', '20251002'])).length === 0);
  check('시세가 하나도 없으면 빈 목록(판단 불가)', M.priceGapDates(cal, new Set()).length === 0);
  let e = mkEnv(cal, ['20251012', '20251006', '20251002']);
  check('DB 확인: 날짜별 1행만 읽어 구멍 2일 발견', JSON.stringify(await M.findPriceGaps(e, 25)) === JSON.stringify(['20251007', '20251008']));
  e = mkEnv(cal, cal);
  check('구멍이 없으면 빈 목록', (await M.findPriceGaps(e, 25)).length === 0);

  // 재시도: 진행 중 작업을 되돌리지 않음 / 멈춘 작업은 다시 시작
  e = mkEnv(cal, ['20251012', '20251006', '20251002'], { append: { startedAt: new Date().toISOString() } });
  let log = await M.runDailyCatchUp(e, 10);
  check('1시간 안에 시작한 일괄 추가가 있으면 새로 시작하지 않음(universe 호출 0)', calls.uni === 0 && log.universe.indexOf('진행 중') >= 0);
  check('그래도 코스피 갱신과 빠진 날 채우기는 수행', calls.kospi === 1 && calls.range.length === 1 && calls.range[0][0] === '20251007' && calls.range[0][1] === '20251008' && log.gapJob.indexOf('2일') >= 0);
  e = mkEnv(cal, ['20251012', '20251006', '20251002'], { append: { startedAt: new Date(Date.now() - 5 * 3600 * 1000).toISOString() } });
  log = await M.runDailyCatchUp(e, 11);
  check('3시간 넘게 남은 일괄 추가는 멈춘 것으로 보고 다시 시작', calls.uni === 1 && log.universe.indexOf('시작(20251012)') >= 0);
  const saved = e.db.prepare("SELECT v FROM market_job WHERE k = 'daily_log'").get();
  check('실행 결과가 daily_log로 저장됨(상태 화면용)', saved && JSON.parse(saved.v).basDt === '20251012');

  // 실패 격리
  globalThis.__stub.uniErr = '공공데이터 오류'; globalThis.__stub.kErr = '지수 오류';
  calls.range.length = 0; e = mkEnv(cal, ['20251012', '20251006', '20251002']);
  log = await M.runDailyCatchUp(e, 9);
  check('시세·코스피가 실패해도 기록하고 빠진 날 확인은 계속', log.universe.indexOf('실패') === 0 && log.kospi.indexOf('실패') === 0 && calls.range.length === 1);
  globalThis.__stub.uniErr = null; globalThis.__stub.kErr = null; globalThis.__stub.rangeRes = { error: '이미 진행 중인 기간 수집이 있습니다', status: 409 };
  e = mkEnv(cal, ['20251012', '20251006', '20251002']);
  log = await M.runDailyCatchUp(e, 12);
  check('기간 수집이 이미 진행 중이면 오류를 기록만 하고 넘어감', log.gapJob.indexOf('시작 못 함') === 0);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과'); process.exit(fail ? 1 : 0);
})().catch((x) => { console.error(x); process.exit(1); });
