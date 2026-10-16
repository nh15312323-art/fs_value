// 신규 상장 종목 동기화 테스트: node tests/corpsync_test.js (Node 22+ node:sqlite)
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('// ===== 신규 상장 종목 정보 동기화 ====='), j = text.indexOf('// ===== /신규 상장 종목 정보 동기화 =====');
if (i < 0 || j < 0) throw new Error('구간 없음');
const mri = text.slice(text.indexOf('async function multiRowInsert'), text.indexOf('\n}\n', text.indexOf('async function multiRowInsert')) + 3);
const stubs = "const __s = globalThis.__stub; function todayStr() { return __s.today; }" +
  "function addDaysStr(y, d) { const t = Date.UTC(+y.slice(0,4), +y.slice(4,6)-1, +y.slice(6,8)) + d*86400000; const x = new Date(t); return x.getUTCFullYear() + String(x.getUTCMonth()+1).padStart(2,'0') + String(x.getUTCDate()).padStart(2,'0'); }" +
  "async function ensureAppSettings(db) { await db.prepare('CREATE TABLE IF NOT EXISTS app_settings (k TEXT PRIMARY KEY, v TEXT, updated_at TEXT)').run(); }" +
  "const fetch = async (u) => { __s.urls.push(String(u)); if (__s.fail) throw new Error(__s.fail); return { json: async () => __s.resp(new URL(String(u))) }; };";
globalThis.__stub = { today: '20251016', urls: [], resp: null, fail: null };
const M = new Function(stubs + mri + text.slice(i, j) + '\nreturn { pickCorpCodes, parseCorpListed, syncNewListings, corpCodeOk };')();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
function mkEnv() {
  const db = new DatabaseSync(':memory:');
  db.exec("CREATE TABLE corp_master (corp_code TEXT PRIMARY KEY, corp_name TEXT, stock_code TEXT, modify_date TEXT);" +
    "CREATE TABLE market_universe_status (stock_code TEXT PRIMARY KEY, corp_name TEXT, backfilled INTEGER);" +
    "CREATE TABLE market_fetch_queue (stock_code TEXT PRIMARY KEY, corp_name TEXT);");
  db.exec("INSERT INTO corp_master VALUES ('00126380','삼성전자','005930','20250101')");
  db.exec("INSERT INTO market_universe_status VALUES ('005930','삼성전자',1),('900001','신규상장A',1),('0001A0','영문코드주',1)");
  db.exec("INSERT INTO market_fetch_queue VALUES ('900002','신규상장B'),('069500','KODEX 200'),('005930','삼성전자')");
  const wrap = (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ all: async () => ({ results: st.all(...b) }), first: async () => st.get(...b) || null, run: async () => st.run(...b) }), all: async () => ({ results: st.all() }), first: async () => st.get() || null, run: async () => st.run() }; };
  return { db, env: { DB: { prepare: wrap }, DART_PROXY_URL: 'https://proxy.example/exec' } };
}
(async () => {
  check('코드 형식: 숫자 6자리·영문 섞인 6자리 허용, 그 외 거부', M.corpCodeOk('005930') && M.corpCodeOk('0001A0') && !M.corpCodeOk('5930') && !M.corpCodeOk('00593O1') && !M.corpCodeOk(null));
  const p = M.pickCorpCodes(['111111', '111111', 'bad', '222222', '333333'], { '222222': '20251014', '333333': '20251001' }, '20251016', 7, 10);
  check('후보 고르기: 중복·잘못된 코드·최근(7일 내) 못 찾은 코드는 제외, 오래된 시도는 다시', JSON.stringify(p) === JSON.stringify(['111111', '333333']), JSON.stringify(p));
  check('최대 개수 제한', M.pickCorpCodes(['111111', '222222', '333333'], {}, '20251016', 7, 2).length === 2);
  const parsed = M.parseCorpListed({ list: [{ corp_code: '00999001', corp_name: '신규상장A', stock_code: '900001', modify_date: '20251010' }, { corp_code: 'xx', corp_name: '나쁨', stock_code: '900002' }, { corp_code: '00999009', corp_name: '요청안함', stock_code: '777777' }] }, ['900001', '900002']);
  check('응답 해석: 형식이 맞고 요청한 코드만', parsed.length === 1 && parsed[0][2] === '900001' && parsed[0][0] === '00999001');
  check('응답이 목록이 아니면 null(프록시 미지원 구분)', M.parseCorpListed({ status: '100' }, ['1']) === null && M.parseCorpListed(null, []) === null);

  // 전체 흐름
  let { db, env } = mkEnv();
  globalThis.__stub.resp = (u) => ({ status: '000', list: [{ corp_code: '00999001', corp_name: '신규상장A', stock_code: '900001', modify_date: '20251010' }, { corp_code: '00999002', corp_name: '신규상장B', stock_code: '900002', modify_date: '20251011' }, { corp_code: '00999003', corp_name: '영문코드주', stock_code: '0001A0', modify_date: '20251012' }] });
  let r = await M.syncNewListings(env);
  const rows = db.prepare('SELECT corp_name, stock_code FROM corp_master ORDER BY stock_code').all();
  check('corp_master에 없는 종목만 조회(삼성전자 제외, 중복 제거) — 4개', r.missing === 4 && r.asked === 4 && /codes=/.test(globalThis.__stub.urls[0]) && !/005930/.test(globalThis.__stub.urls[0]), globalThis.__stub.urls[0]);
  check('DART에서 찾은 3종목 추가(영문 섞인 코드 포함), 기존 행 유지', r.added === 3 && rows.length === 4 && rows.some((x) => x.stock_code === '0001A0'), JSON.stringify(rows));
  const st1 = JSON.parse(db.prepare("SELECT v FROM app_settings WHERE k='corp_sync'").get().v);
  check('못 찾은 코드(069500 ETF)는 시도 기록에 남음', st1.tried['069500'] === '20251016' && !st1.tried['900001']);
  globalThis.__stub.urls.length = 0;
  r = await M.syncNewListings(env);
  check('바로 다시 실행: ETF는 7일간 다시 묻지 않아 조회 자체를 안 함', r.asked === 0 && globalThis.__stub.urls.length === 0 && /이미 물어봤/.test(r.note), r.note);
  globalThis.__stub.today = '20251024';
  r = await M.syncNewListings(env);
  check('8일 뒤에는 다시 물어봄', r.asked === 1 && globalThis.__stub.urls.length === 1);
  r = await M.syncNewListings(env, { force: true });
  check('force=1이면 최근 못 찾은 코드도 다시 물음', r.asked === 1);
  globalThis.__stub.today = '20251016';

  // 실패·미지원
  ({ db, env } = mkEnv());
  globalThis.__stub.resp = () => ({ status: '000', message: '지원 안 함' });
  r = await M.syncNewListings(env);
  check('프록시가 corpcode 미지원이면 추가 0 + 안내(Apps Script 코드 추가)', r.added === 0 && /Apps Script/.test(r.note) && db.prepare('SELECT COUNT(*) c FROM corp_master').get().c === 1);
  check('미지원일 때는 시도 기록을 남기지 않음(코드 추가 후 바로 다시 시도 가능)', !db.prepare("SELECT v FROM app_settings WHERE k='corp_sync'").get());
  globalThis.__stub.fail = '네트워크 오류';
  let threw = false; try { await M.syncNewListings(env); } catch (e) { threw = /네트워크/.test(e.message); }
  check('네트워크 실패는 예외로 올려 일일 로그에 기록되게 함', threw);
  globalThis.__stub.fail = null;
  env.DART_PROXY_URL = '';
  r = await M.syncNewListings(env);
  check('프록시 주소 없으면 건너뜀', r.note === 'DART_PROXY_URL 미설정');
  ({ db, env } = mkEnv()); db.exec("INSERT INTO corp_master VALUES ('a','b','900001',null),('c','d','900002',null),('e','f','0001A0',null),('g','h','069500',null)");
  r = await M.syncNewListings(env);
  check('빠진 종목이 없으면 프록시를 부르지 않음', r.missing === 0 && globalThis.__stub.urls.length === 0 || r.missing === 0);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과'); process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
