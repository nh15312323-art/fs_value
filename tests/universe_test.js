// 사용법: node tests/universe_test.js  (Node 22+ : node:sqlite)
// 저장 기준 설정(/api/market/config)·미리보기·기간 지정 시세 수집(/api/market/range, cron 틱)을 메모리 SQLite(가짜 D1)와 가짜 공공데이터로 검증한다.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const calls = []; let krx = async () => ({ items: [], totalCount: 0 });
const code = [
  'const MARKET_MIN_TRADING_VALUE = 1_500_000_000, MARKET_MIN_MCAP = 50_000_000_000, MARKET_DAILY_WRITE_CAP = 150;',
  between('function todayStr()', '\n}\n') + '\n}\n',
  between('function addDaysStr(', '\n}\n') + '\n}\n',
  between('async function multiRowInsert(', '\n}\n') + '\n}\n',
  between('function pfAuthError(', 'const PF_STATUSES'),
  'async function ensureMarketQueueColumns() {}',
  'async function fetchKrxPage(p) { calls.push(p); return krx(p); }',
  between('const UNIVERSE_LIMITS', 'async function runUniverseRefresh('),
  'return { getUniverseCfg, handleMarketCfg, startRangeJob, processRangeJobTick, rangeStatus, RANGE_DAILY_ROW_CAP, RANGE_PAGE };',
].join('\n');
const U = new Function('calls', 'get', code.replace(/\bkrx\b/g, 'get()'))(calls, () => krx);
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };

function mkEnv(extra) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, volume REAL, trading_value REAL, market_cap REAL, PRIMARY KEY (market_date, stock_code));' +
    'CREATE TABLE market_job (k TEXT PRIMARY KEY, v TEXT); CREATE TABLE app_settings (k TEXT PRIMARY KEY, v TEXT, updated_at TEXT); CREATE TABLE market_universe_status (stock_code TEXT PRIMARY KEY, corp_name TEXT, backfilled INTEGER);');
  const DB = { prepare: (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ all: async () => ({ results: st.all(...b) }), first: async () => st.get(...b) || null, run: async () => { const r = st.run(...b); return { meta: { changes: r.changes } }; } }), all: async () => ({ results: st.all() }), first: async () => st.get() || null, run: async () => { const r = st.run(); return { meta: { changes: r.changes } }; } }; } };
  return Object.assign({ db, DB, DATA_GO_KR_KEY: 'k' }, extra || {});
}
const item = (d, c, mc, tv) => ({ basDt: d, srtnCd: c, itmsNm: '종목' + c, clpr: 1000, trqu: 10, trPrc: tv == null ? 2e9 : tv, mrktTotAmt: mc });
const req = (method, p, body, key) => new Request('http://x' + p, { method, headers: Object.assign({ 'content-type': 'application/json' }, key ? { 'x-pf-key': key } : {}), body: body ? JSON.stringify(body) : undefined });
const call = async (env, method, p, body, key) => { const u = new URL('http://x' + p); const r = await U.handleMarketCfg(req(method, p, body, key), env, u.pathname, u.searchParams); return { status: r.status, data: await r.json() }; };

(async () => {
  let env = mkEnv(); let r;
  let c = await U.getUniverseCfg(env, true);
  check('설정이 없으면 기본값(거래대금 15억·시총 500억), custom=false', c.minTvEok === 15 && c.minMcapEok === 500 && c.minTv === 1.5e9 && c.minMcap === 5e10 && c.custom === false);
  r = await call(env, 'POST', '/api/market/config', { min_tv_eok: -1, min_mcap_eok: 500 });
  check('음수 거래대금은 400', r.status === 400, r.data.error);
  r = await call(env, 'POST', '/api/market/config', { min_tv_eok: 5, min_mcap_eok: '' });
  check('빈 시가총액은 400', r.status === 400);
  r = await call(env, 'POST', '/api/market/config', { min_tv_eok: 0, min_mcap_eok: 300 });
  check('거래대금 0도 허용, 저장 성공', r.status === 200 && r.data.cfg.minTvEok === 0 && r.data.cfg.minMcapEok === 300 && r.data.cfg.custom === true);
  c = await U.getUniverseCfg(mkEnv.last = env, true);
  check('저장된 값을 다시 읽으면 그대로(원 단위 환산 포함)', c.minTv === 0 && c.minMcap === 3e10);
  r = await call(env, 'GET', '/api/market/config');
  check('GET: 현재값·기본값·보호 여부', r.data.cfg.minMcapEok === 300 && r.data.defaults.minMcapEok === 500 && r.data.protected === false);
  const envK = mkEnv({ PF_KEY: 'abcdefghijklmnop' });
  r = await call(envK, 'POST', '/api/market/config', { min_tv_eok: 5, min_mcap_eok: 300 });
  check('PF_KEY가 설정돼 있으면 키 없이 변경 불가(401)', r.status === 401);
  r = await call(envK, 'POST', '/api/market/config', { min_tv_eok: 5, min_mcap_eok: 300 }, 'abcdefghijklmnop');
  check('올바른 키면 변경 가능', r.status === 200, JSON.stringify(r.data));
  r = await call(envK, 'GET', '/api/market/config');
  check('조회는 키 없이 가능, protected=true', r.status === 200 && r.data.protected === true);

  // 미리보기
  env = mkEnv(); env.db.exec("INSERT INTO market_raw_daily VALUES ('20250110','A',1,1,1,1)");
  krx = async (p) => ({ items: [item('20250110', '000010', 6e10), item('20250110', '000020', 2e10), item('20250110', '000030', 9e10)], totalCount: 3, resultCode: '00' });
  r = await call(env, 'GET', '/api/market/preview?tv=10&mcap=500');
  check('미리보기: 시총 500억 미만은 제외, 통과 코드 반환', r.status === 200 && r.data.pass.join() === '000010,000030' && r.data.done === true && r.data.bas_dt === '20250110');
  check('미리보기: 거래대금 하한(억→원)을 공공데이터 조건으로 전달', calls[calls.length - 1].beginTrPrc === 10e8);
  env.db.exec("INSERT INTO market_universe_status VALUES ('000010','x',1),('999','y',0)");
  r = await call(env, 'GET', '/api/market/preview?mode=known');
  check('이미 이력이 있는 종목 목록(백필 완료만)', r.data.codes.join() === '000010');
  r = await call(env, 'GET', '/api/market/preview?tv=abc&mcap=1');
  check('잘못된 입력은 400', r.status === 400);

  // 기간 수집 시작 검증
  env = mkEnv();
  check('날짜 형식 오류', (await U.startRangeJob(env, '2025-01-06', '20250110')).status === 400);
  check('시작일 > 종료일', (await U.startRangeJob(env, '20250110', '20250106')).status === 400);
  check('미래 종료일 거부', (await U.startRangeJob(env, '20250106', '20991231')).status === 400);
  check('400일 초과 거부', (await U.startRangeJob(env, '20230101', '20250101')).status === 400);
  check('존재하지 않는 날짜(2월 30일) 거부', (await U.startRangeJob(env, '20250230', '20250301')).status === 400);

  // 틱 처리: 월(6)~일(12). 화(7)=휴장, 수(8)=2쪽(300행), 목(9)=소수, 금(10)=저시총 포함
  env.db.exec("INSERT INTO market_raw_daily VALUES ('20250108','C000',555,1,1,NULL)"); // 시총 없는 기존 행 → 시총만 채워져야 함
  env.db.exec("INSERT INTO market_raw_daily VALUES ('20250109','C001',777,1,1,1234)"); // 이미 완전한 행 → 그대로
  r = await U.startRangeJob(env, '20250106', '20250112');
  check('시작: 설정 스냅샷(기본 기준)과 함께 작업 등록', r.ok && r.job.minTv === 1.5e9 && r.job.minMcap === 5e10 && r.job.cur === '20250106');
  check('진행 중에 또 시작하면 409', (await U.startRangeJob(env, '20250106', '20250107')).status === 409);
  const big = Array.from({ length: 300 }, (_, i) => item('20250108', 'C' + String(i).padStart(3, '0'), 9e10));
  krx = async (p) => {
    if (p.basDt === '20250107') return { items: [], totalCount: 0, resultCode: '00' };
    if (p.basDt === '20250108') return { items: big.slice((p.pageNo - 1) * p.numOfRows, p.pageNo * p.numOfRows), totalCount: 300, resultCode: '00' };
    if (p.basDt === '20250109') return { items: [item('20250109', 'C001', 9e10)], totalCount: 1, resultCode: '00' };
    if (p.basDt === '20250110') return { items: [item('20250110', 'D1', 9e10), item('20250110', 'D2', 1e10)], totalCount: 2, resultCode: '00' };
    return { items: [item(p.basDt, 'Z', 9e10)], totalCount: 1, resultCode: '00' };
  };
  calls.length = 0;
  let n = 0; while (await U.processRangeJobTick(env) && n < 30) n++;
  const st = await U.rangeStatus(env);
  check('끝까지 처리하고 done=true', st.done === true && !st.error, JSON.stringify(st));
  check('주말(토·일)은 공공데이터를 호출하지 않음', calls.every((p) => !['20250111', '20250112'].includes(p.basDt)), calls.map((p) => p.basDt + ':' + p.pageNo).join(' '));
  check('휴장일(화)은 비어 있어 건너뜀 1일 기록, 거래일 4일(월·수·목·금) 처리', st.skipped === 1 && st.tradingDates === 4, st.skipped + '/' + st.tradingDates);
  check('300행 날짜는 2쪽에 걸쳐 모두 저장', env.db.prepare("SELECT COUNT(*) c FROM market_raw_daily WHERE market_date='20250108'").get().c === 300);
  check('시총 500억 미만(D2)은 저장하지 않음', env.db.prepare("SELECT COUNT(*) c FROM market_raw_daily WHERE stock_code='D2'").get().c === 0 && env.db.prepare("SELECT COUNT(*) c FROM market_raw_daily WHERE stock_code='D1'").get().c === 1);
  check('시총이 비어 있던 기존 행은 시총만 채우고 종가는 유지', (() => { const x = env.db.prepare("SELECT close_price, market_cap FROM market_raw_daily WHERE market_date='20250108' AND stock_code='C000'").get(); return x.close_price === 555 && x.market_cap === 9e10; })());
  check('이미 완전한 기존 행은 그대로(종가 777 유지)', env.db.prepare("SELECT close_price, market_cap FROM market_raw_daily WHERE market_date='20250109' AND stock_code='C001'").get().close_price === 777);
  check('완료된 작업은 더 처리하지 않음', (await U.processRangeJobTick(env)) === false);

  // 하루 상한
  env = mkEnv(); await U.startRangeJob(env, '20250106', '20250110');
  let j = await U.rangeStatus(env); j.budgetRows = U.RANGE_DAILY_ROW_CAP; env.db.prepare("UPDATE market_job SET v=? WHERE k='range'").run(JSON.stringify(j));
  calls.length = 0;
  check('오늘 몫(15,000행)을 다 쓰면 호출 없이 양보(false)', (await U.processRangeJobTick(env)) === false && calls.length === 0);
  j.budgetDay = '20200101'; env.db.prepare("UPDATE market_job SET v=? WHERE k='range'").run(JSON.stringify(j));
  check('날짜가 바뀌면 한도가 초기화되어 다시 처리', (await U.processRangeJobTick(env)) === true && calls.length === 1);

  // 연속 실패 → 중단
  env = mkEnv(); await U.startRangeJob(env, '20250106', '20250110');
  krx = async () => { throw new Error('boom'); };
  for (let k = 0; k < 5; k++) await U.processRangeJobTick(env);
  j = await U.rangeStatus(env);
  check('연속 5회 실패하면 중단하고 사유 기록', j.done === true && /연속 실패/.test(j.error));

  // 취소 + 키 보호
  env = mkEnv({ PF_KEY: 'abcdefghijklmnop' });
  r = await call(env, 'POST', '/api/market/range', { from: '2025-01-06', to: '2025-01-07' });
  check('PF_KEY 설정 시 키 없이 기간 수집 시작 불가', r.status === 401);
  r = await call(env, 'POST', '/api/market/range', { from: '2025-01-06', to: '2025-01-07' }, 'abcdefghijklmnop');
  check('키가 있으면 시작(날짜 하이픈 형식 허용)', r.status === 200 && r.data.job.from === '20250106');
  r = await call(env, 'POST', '/api/market/range', { cancel: true }, 'abcdefghijklmnop');
  r = await call(env, 'GET', '/api/market/range');
  check('취소하면 진행 작업이 사라짐', r.data.job === null);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
})();
